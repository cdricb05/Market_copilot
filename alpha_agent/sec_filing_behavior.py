"""alpha_agent/sec_filing_behavior.py — Release-27 canonical reader for
**SEC reporting-BEHAVIOUR** observables.

Stage 26 acquired the free SEC Financial Statement Data Sets and used exactly one
column of ``sub.txt`` — the assigned SIC — to build a leakage-safe point-in-time
sector tier. The same 46,680 acquired submissions also carry ``form``, ``period``,
``filed``, ``accepted``, ``afs`` and ``prevrpt``, and the owned companyfacts index
already keys every fact by ``accession``. Those two surfaces describe *how a
company reports*, which is information about governance and stress rather than
information about the reported numbers, and neither has ever been tested here.

This module is the ONE reader those families share. Stage 26 closed with three
runnable families and the release contract forbids three separate parsers, so
everything that answers "what could an observer have known about this issuer's
reporting behaviour on date D" lives here:

* :class:`FilingHistory` — per-issuer submission history from the acquired
  ``sub.txt`` members: filing lag, own-history abnormal lag, statutory-deadline
  misses, amendment events, acceptance-time-of-day, cadence disruption and
  fiscal-calendar changes.
* :class:`FactRevisionHistory` — per-issuer **fact revision** events derived from
  the owned companyfacts index by comparing the value of the SAME accounting
  concept, for the SAME reporting duration, across DIFFERENT accessions.
* :class:`ShareDynamicsHistory` — split-normalised net share issuance, composed
  from the released :mod:`alpha_agent.pit_market_equity` owners rather than a
  second share reader.

Three point-in-time rules are enforced here and are the reason the families are
admissible at all:

1. **A submission is observable only at its own SEC acceptance timestamp.** Never
   at its period end, and never at the acceptance timestamp of a later filing.
2. **``prevrpt`` is refused as a signal input.** The column means "this
   submission was amended before the end cutoff of the data set it appears in",
   which is a *retroactive* flag carrying up to a quarter of look-ahead. An
   amendment is admitted only as its own submission, at its own acceptance time.
3. **A fact revision is observable only at the FILED date of the later
   accession**, and is a revision only when the value actually moved for an
   identical ``(concept, unit, period_start, period_end)`` context. Restating the
   same number in a later comparative column is not a restatement.

Pure standard library plus the released owners. Research-only, read-only: no
operational store, no order, no model, no network.
"""
from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Optional

CONTRACT_VERSION = "release27-sec-filing-behavior-1.0.0"

# --------------------------------------------------------------------------- #
# Form taxonomy.
# --------------------------------------------------------------------------- #
#: Periodic reports whose statutory deadline is defined and whose lag is
#: therefore economically interpretable. Transition reports (10-KT / 10-QT) cover
#: an irregular period by construction and are deliberately excluded from every
#: lag statistic; they are still counted as filings for cadence purposes.
ANNUAL_FORMS = frozenset({"10-K"})
QUARTERLY_FORMS = frozenset({"10-Q"})
PERIODIC_FORMS = ANNUAL_FORMS | QUARTERLY_FORMS
TRANSITION_FORMS = frozenset({"10-KT", "10-QT"})
ANNUAL_AMENDMENTS = frozenset({"10-K/A", "10-KT/A"})
QUARTERLY_AMENDMENTS = frozenset({"10-Q/A", "10-QT/A"})
AMENDMENT_FORMS = ANNUAL_AMENDMENTS | QUARTERLY_AMENDMENTS

#: Statutory filing deadlines in calendar days, by SEC filer status (``afs``).
#: These are the published Exchange Act deadlines, not fitted parameters:
#: large accelerated 60/40, accelerated 75/40, everyone else 90/45.
DEADLINE_DAYS = {
    "1-LAF": {"annual": 60, "quarterly": 40},
    "2-ACC": {"annual": 75, "quarterly": 40},
    "3-ACC": {"annual": 75, "quarterly": 40},
    "4-NON": {"annual": 90, "quarterly": 45},
    "5-SML": {"annual": 90, "quarterly": 45},
}
DEFAULT_DEADLINE = {"annual": 90, "quarterly": 45}

#: Rule 0-3 rolls a deadline falling on a weekend or federal holiday to the next
#: business day, and this module does not carry a holiday calendar. A fixed
#: four-day grace absorbs that roll so a weekend is never counted as lateness.
#: It is a calendar correction, declared before any result exists, and is never
#: tuned against an outcome.
DEADLINE_GRACE_DAYS = 4

#: Regular-session close in the Eastern time zone EDGAR timestamps are stamped
#: in. A filing accepted at or after this hour was published to a market that
#: could not trade on it until the next session.
MARKET_CLOSE_HOUR_ET = 16

#: Trailing windows, in days, fixed here before any outcome is observed.
WINDOW_1Y = 365
WINDOW_2Y = 730
WINDOW_3Y = 1095

#: An issuer's own-history lag benchmark needs enough prior filings of the same
#: form to be a benchmark rather than noise.
MIN_OWN_HISTORY_FILINGS = 4

#: Cadence: a periodic reporter is expected to file at least quarterly. A gap
#: materially longer than a quarter is a disruption, not a calendar artefact.
EXPECTED_CADENCE_DAYS = 92


def _d(value) -> Optional[date]:
    """``YYYYMMDD`` or ``YYYY-MM-DD`` to a date; anything else to ``None``."""
    s = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(s) < 8:
        return None
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except ValueError:
        return None


def _iso(d: Optional[date]) -> Optional[str]:
    return d.isoformat() if d is not None else None


def _median(xs):
    v = sorted(x for x in xs if x is not None)
    if not v:
        return None
    n = len(v)
    return float(v[n // 2]) if n % 2 else (v[n // 2 - 1] + v[n // 2]) / 2.0


def _mean(xs):
    v = [x for x in xs if x is not None]
    return (sum(v) / len(v)) if v else None


# =========================================================================== #
# 1. Filing history — the ``sub.txt`` submission stream, point-in-time.
# =========================================================================== #
class FilingHistory:
    """Per-issuer SEC submission history, queried strictly as-of.

    Built from the ``sub.txt`` members Stage 26 already acquired and cached, so
    this class performs no network access and acquires nothing. It owns the
    *behavioural* projection of a submission; the ``(sic, accepted)`` projection
    stays owned by :mod:`alpha_agent.sec_financial_statement_sets`.
    """

    def __init__(self, cache_root: "str | Path") -> None:
        self.cache_root = Path(cache_root)
        #: cik -> list of submission dicts sorted by (observable_at, adsh)
        self._by_cik: "dict[str, list[dict]]" = {}
        self.load_status: dict = {}
        self._quarters: "list[str]" = []
        self._prevrpt_rows = 0

    # -- loading ------------------------------------------------------------- #
    def load(self, ciks: Optional[Iterable[str]] = None) -> dict:
        from . import sec_financial_statement_sets as _fsds

        if not self.cache_root.exists():
            self.load_status = {"ok": False, "reason": "FSDS_CACHE_ABSENT",
                                "path": str(self.cache_root)}
            return self.load_status
        wanted = {str(int(str(c).lstrip("0") or "0")) for c in (ciks or []) if c} \
            or None
        quarters: "list[str]" = []
        kept = 0
        seen_adsh: set = set()
        for qdir in sorted(p for p in self.cache_root.iterdir() if p.is_dir()):
            member = qdir / _fsds.MEMBER_NAME
            if not member.exists():
                continue
            quarters.append(qdir.name)
            for row in _fsds.parse_sub_txt(member.read_bytes()):
                sub = self._project(row, quarter=qdir.name)
                if sub is None:
                    continue
                if wanted is not None and sub["cik"] not in wanted:
                    continue
                # The same submission can legitimately appear in two consecutive
                # quarterly data sets. The accession is the identity, and the
                # FIRST observation of it is authoritative: a later re-listing
                # carries the later data set's retroactive `prevrpt`.
                if sub["adsh"] in seen_adsh:
                    continue
                seen_adsh.add(sub["adsh"])
                if sub.get("prevrpt"):
                    self._prevrpt_rows += 1
                self._by_cik.setdefault(sub["cik"], []).append(sub)
                kept += 1
        for subs in self._by_cik.values():
            subs.sort(key=lambda s: (s["observable_at"], s["adsh"]))
        self._quarters = quarters
        forms: "dict[str, int]" = {}
        for subs in self._by_cik.values():
            for s in subs:
                forms[s["form"]] = forms.get(s["form"], 0) + 1
        self.load_status = {
            "ok": bool(kept), "path": str(self.cache_root),
            "quarters_loaded": len(quarters),
            "first_quarter": quarters[0] if quarters else None,
            "last_quarter": quarters[-1] if quarters else None,
            "submissions": kept, "issuers": len(self._by_cik),
            "acceptance_timestamped": sum(
                1 for subs in self._by_cik.values() for s in subs
                if s["availability_basis"] == "SEC_ACCEPTANCE"),
            "prevrpt_rows_seen": self._prevrpt_rows,
            "prevrpt_policy": "DIAGNOSTIC_ONLY_NEVER_A_SIGNAL_INPUT",
            "forms_top": dict(sorted(forms.items(), key=lambda kv: -kv[1])[:12]),
            "evidence_class": "LEAKAGE_SAFE_PIT_SEC_SUBMISSION_STREAM",
            "contract_version": CONTRACT_VERSION,
        }
        return self.load_status

    @staticmethod
    def _project(row: dict, *, quarter: str) -> Optional[dict]:
        cik = "".join(ch for ch in str(row.get("cik") or "") if ch.isdigit())
        adsh = (row.get("adsh") or "").strip()
        if not cik or not adsh:
            return None
        form = (row.get("form") or "").strip().upper()
        period = _d(row.get("period"))
        filed = _d(row.get("filed"))
        accepted_raw = (row.get("accepted") or "").strip()
        accepted_date, accepted_hour = None, None
        if len(accepted_raw) >= 10:
            accepted_date = _d(accepted_raw[:10])
            tail = accepted_raw[11:] if len(accepted_raw) > 11 else ""
            if len(tail) >= 2 and tail[:2].isdigit():
                accepted_hour = int(tail[:2])
        basis = "SEC_ACCEPTANCE" if accepted_date is not None else "SEC_FILED_DATE"
        observable = accepted_date or filed
        if observable is None:
            return None
        return {
            "cik": str(int(cik)),
            "adsh": adsh,
            "form": form,
            "period": _iso(period),
            "period_d": period,
            "filed": _iso(filed),
            "filed_d": filed,
            "accepted_date": _iso(accepted_date),
            "accepted_hour_et": accepted_hour,
            "observable_at": _iso(observable),
            "observable_d": observable,
            "availability_basis": basis,
            "afs": (row.get("afs") or "").strip(),
            "fye": (row.get("fye") or "").strip(),
            "fy": (row.get("fy") or "").strip(),
            "fp": (row.get("fp") or "").strip(),
            "detail": (row.get("detail") or "").strip(),
            "nciks": (row.get("nciks") or "").strip(),
            # Retained for the diagnostic only. Rule 2 in the module docstring.
            "prevrpt": (row.get("prevrpt") or "").strip() in ("1", "TRUE", "true"),
            "quarter": quarter,
        }

    # -- point-in-time query -------------------------------------------------- #
    def submissions_as_of(self, cik: str, as_of: str) -> "list[dict]":
        """Every submission ACCEPTED on or before ``as_of``, oldest first."""
        subs = self._by_cik.get(str(int(str(cik).lstrip("0") or "0")))
        if not subs:
            return []
        cutoff = str(as_of)[:10]
        out = []
        for s in subs:
            if s["observable_at"] > cutoff:
                break            # sorted by observable_at
            out.append(s)
        return out

    def covered_ciks(self) -> set:
        return set(self._by_cik)

    # -- the behavioural projection ------------------------------------------- #
    def observables(self, cik: str, as_of: str) -> Optional[dict]:
        """Every primitive filing-behaviour observable at ``as_of``.

        Returns ``None`` when the issuer has no observable submission at all;
        an individual observable that cannot be computed is ``None`` rather than
        zero, so a missing history never masquerades as prompt reporting.
        """
        subs = self.submissions_as_of(cik, as_of)
        if not subs:
            return None
        cutoff = _d(as_of)
        annual = [s for s in subs if s["form"] in ANNUAL_FORMS]
        quarterly = [s for s in subs if s["form"] in QUARTERLY_FORMS]
        periodic = [s for s in subs
                    if s["form"] in PERIODIC_FORMS or s["form"] in TRANSITION_FORMS]
        amendments = [s for s in subs if s["form"] in AMENDMENT_FORMS]

        out: dict = {
            "submissions_observed": len(subs),
            "annual_observed": len(annual),
            "quarterly_observed": len(quarterly),
            "amendments_observed": len(amendments),
            "filer_status": subs[-1].get("afs") or None,
            "last_observable_at": subs[-1]["observable_at"],
        }

        # -- filing lag, levels and own-history abnormality -------------------- #
        out["annual_lag_days"] = _lag(annual[-1]) if annual else None
        out["quarterly_lag_days"] = _lag(quarterly[-1]) if quarterly else None
        out["annual_lag_prior"] = _lag(annual[-2]) if len(annual) >= 2 else None
        out["quarterly_lag_prior"] = (_lag(quarterly[-2])
                                      if len(quarterly) >= 2 else None)
        out["annual_lag_change"] = _sub(out["annual_lag_days"],
                                        out["annual_lag_prior"])
        out["quarterly_lag_change"] = _sub(out["quarterly_lag_days"],
                                           out["quarterly_lag_prior"])

        prior_annual_lags = [_lag(s) for s in annual[:-1]]
        prior_annual_lags = [x for x in prior_annual_lags if x is not None]
        out["annual_lag_own_median"] = (
            _median(prior_annual_lags)
            if len(prior_annual_lags) >= MIN_OWN_HISTORY_FILINGS else None)
        out["annual_lag_abnormal"] = _sub(out["annual_lag_days"],
                                          out["annual_lag_own_median"])
        prior_q_lags = [_lag(s) for s in quarterly[:-1]]
        prior_q_lags = [x for x in prior_q_lags if x is not None]
        out["quarterly_lag_own_median"] = (
            _median(prior_q_lags)
            if len(prior_q_lags) >= MIN_OWN_HISTORY_FILINGS else None)
        out["quarterly_lag_abnormal"] = _sub(out["quarterly_lag_days"],
                                             out["quarterly_lag_own_median"])

        # Trailing quarterly-lag trend: the mean of the four most recent
        # quarterly lags minus the mean of the four before them. A deteriorating
        # reporting process shows up here before it breaches a deadline.
        recent_q = [_lag(s) for s in quarterly[-4:]]
        older_q = [_lag(s) for s in quarterly[-8:-4]]
        out["quarterly_lag_trend"] = (
            _sub(_mean(recent_q), _mean(older_q))
            if len(quarterly) >= 8 else None)

        # -- statutory deadline behaviour -------------------------------------- #
        recent_periodic = [s for s in periodic
                           if s["form"] in PERIODIC_FORMS][-8:]
        misses = [_deadline_miss(s) for s in recent_periodic]
        misses = [m for m in misses if m is not None]
        out["deadline_miss_rate_8"] = (
            (sum(1 for m in misses if m > 0) / len(misses)) if misses else None)
        out["deadline_worst_overrun_8"] = (max(misses) if misses else None)
        out["deadline_miss_latest"] = (
            (1.0 if (misses[-1] or 0) > 0 else 0.0) if misses else None)

        # -- cadence disruption -------------------------------------------------- #
        if periodic and cutoff is not None:
            last = periodic[-1]["observable_d"]
            gap = (cutoff - last).days if last else None
            out["days_since_last_periodic"] = gap
            out["cadence_overrun_days"] = (
                max(0, gap - EXPECTED_CADENCE_DAYS) if gap is not None else None)
        else:
            out["days_since_last_periodic"] = None
            out["cadence_overrun_days"] = None

        # -- strategic acceptance timing ---------------------------------------- #
        hours = [s["accepted_hour_et"] for s in periodic[-8:]
                 if s["accepted_hour_et"] is not None]
        out["after_close_rate_8"] = (
            (sum(1 for h in hours if h >= MARKET_CLOSE_HOUR_ET) / len(hours))
            if hours else None)
        out["latest_accepted_hour_et"] = (
            periodic[-1]["accepted_hour_et"] if periodic else None)

        # -- amendment events (the leakage-safe definition) ---------------------- #
        out.update(self._amendment_observables(subs, amendments, cutoff))

        # -- fiscal-calendar change ---------------------------------------------- #
        fyes = [s["fye"] for s in periodic if s.get("fye")]
        out["fiscal_year_end_changes"] = (
            len({f for f in fyes}) - 1 if fyes else None)

        # -- disclosure granularity / co-registrant structure --------------------- #
        details = [s["detail"] for s in periodic[-8:] if s.get("detail") in ("0", "1")]
        out["low_detail_rate_8"] = (
            (sum(1 for x in details if x == "0") / len(details))
            if details else None)
        nciks = [int(s["nciks"]) for s in periodic[-8:]
                 if str(s.get("nciks") or "").isdigit()]
        out["coregistrant_mean_8"] = _mean(nciks)
        return out

    @staticmethod
    def _amendment_observables(subs: "list[dict]", amendments: "list[dict]",
                               cutoff: Optional[date]) -> dict:
        """Amendment counts and latency, each stamped at the AMENDMENT's own
        acceptance timestamp. Never at the original filing's."""
        out = {
            "amendment_count_1y": None, "amendment_count_3y": None,
            "annual_amendment_count_3y": None, "amendment_intensity_3y": None,
            "amendment_recent_1y": None, "distinct_periods_amended_3y": None,
            "amendment_latency_days": None,
        }
        if cutoff is None:
            return out
        w1 = cutoff - timedelta(days=WINDOW_1Y)
        w3 = cutoff - timedelta(days=WINDOW_3Y)

        def _in(sub, lo):
            d = sub["observable_d"]
            return d is not None and lo < d <= cutoff

        a1 = [s for s in amendments if _in(s, w1)]
        a3 = [s for s in amendments if _in(s, w3)]
        periodic3 = [s for s in subs
                     if s["form"] in PERIODIC_FORMS and _in(s, w3)]
        out["amendment_count_1y"] = float(len(a1))
        out["amendment_count_3y"] = float(len(a3))
        out["amendment_recent_1y"] = 1.0 if a1 else 0.0
        out["annual_amendment_count_3y"] = float(
            len([s for s in a3 if s["form"] in ANNUAL_AMENDMENTS]))
        denom = len(periodic3) + len(a3)
        out["amendment_intensity_3y"] = (len(a3) / denom) if denom else None
        out["distinct_periods_amended_3y"] = float(
            len({s["period"] for s in a3 if s["period"]}))

        # Latency: for the most recent amendment, the days between the ORIGINAL
        # submission for that fiscal period (the earliest non-amended periodic
        # filing with the same period) and the amendment's own acceptance.
        if a3:
            amd = a3[-1]
            base_form = amd["form"].split("/")[0]
            originals = [s for s in subs
                         if s["form"] == base_form and s["period"] == amd["period"]
                         and s["observable_d"] is not None
                         and amd["observable_d"] is not None
                         and s["observable_d"] <= amd["observable_d"]]
            if originals:
                out["amendment_latency_days"] = float(
                    (amd["observable_d"] - originals[0]["observable_d"]).days)
        return out

    def prevrpt_diagnostic(self) -> dict:
        """Why the ``prevrpt`` column is refused as a signal input."""
        return {
            "column": "prevrpt",
            "sec_definition": "TRUE indicates that the submission information "
                              "was subsequently amended prior to the end cutoff "
                              "date of the data set",
            "rows_flagged": self._prevrpt_rows,
            "classification": "LOOK_AHEAD_FLAG",
            "why": "the flag is written retroactively against the data set's own "
                   "cutoff, so a submission accepted early in a quarter carries "
                   "up to three months of knowledge an observer did not have on "
                   "its acceptance date",
            "policy": "DIAGNOSTIC_ONLY_NEVER_A_SIGNAL_INPUT — an amendment enters "
                      "this module only as its own submission, at its own "
                      "acceptance timestamp",
        }

    def acquisition_manifest(self) -> dict:
        return {
            "contract_id": "release27_filing_behavior_source_manifest/1",
            "reader_contract": CONTRACT_VERSION,
            "source": "SEC Financial Statement Data Sets, sub.txt",
            "acquired_by": "alpha_agent.sec_financial_statement_sets (Stage 26)",
            "acquired_in_this_campaign": False,
            "network_access_by_this_module": "NONE",
            "cache_root": str(self.cache_root),
            "quarters": self._quarters,
            "quarters_count": len(self._quarters),
            "license_note": "US federal government work; SEC EDGAR public data, "
                            "free for research use under SEC fair-access rules",
        }


def _lag(sub: Optional[dict]) -> Optional[float]:
    """Days from fiscal period end to SEC acceptance for ONE submission."""
    if not sub:
        return None
    p, o = sub.get("period_d"), sub.get("observable_d")
    if p is None or o is None:
        return None
    return float((o - p).days)


def _deadline_miss(sub: Optional[dict]) -> Optional[float]:
    """Days by which a periodic filing overran its statutory deadline.

    Negative means early. The filer status carried by the submission itself is
    used, so a company that becomes a large accelerated filer is held to the
    tighter deadline only from the filing that says so.
    """
    lag = _lag(sub)
    if lag is None:
        return None
    kind = "annual" if sub["form"] in ANNUAL_FORMS else (
        "quarterly" if sub["form"] in QUARTERLY_FORMS else None)
    if kind is None:
        return None
    limit = DEADLINE_DAYS.get(sub.get("afs") or "", DEFAULT_DEADLINE)[kind]
    return lag - (limit + DEADLINE_GRACE_DAYS)


def _sub(a: Optional[float], b: Optional[float]) -> Optional[float]:
    return None if (a is None or b is None) else float(a) - float(b)


# =========================================================================== #
# 2. Fact revisions — the leakage-safe restatement definition.
# =========================================================================== #
#: Core accounting concepts whose revision is economically meaningful. Fixed
#: here, before any outcome exists. Restricting the set is what keeps this a
#: bounded hypothesis rather than a scan over 59 indexed tags.
REVISION_CONCEPTS = (
    "Assets", "Liabilities", "StockholdersEquity", "Revenues", "NetIncomeLoss",
    "OperatingIncomeLoss", "NetCashProvidedByUsedInOperatingActivities",
)

#: A revision counts only when the value moved by more than this fraction of the
#: scale of the two values. Below it the difference is rounding or a units
#: re-presentation, not a correction. Declared before any result; never tuned.
REVISION_MATERIALITY = 0.005

#: Concepts whose sign convention makes "down is worse" economically meaningful.
#: A downward revision of earnings is bad news; a downward revision of
#: liabilities is not, so only these feed the DIRECTIONAL hypothesis.
DIRECTIONAL_CONCEPTS = frozenset({
    "Revenues", "NetIncomeLoss", "OperatingIncomeLoss",
    "NetCashProvidedByUsedInOperatingActivities",
})


class FactRevisionHistory:
    """Per-issuer accounting-fact revision events from the owned companyfacts
    index.

    A revision event is defined for one ``(cik, concept, unit, period_start,
    period_end)`` context: two facts under DIFFERENT accessions whose values
    differ materially. The event is stamped at the FILED date of the later
    accession, which is when the correction became observable.

    Including the reporting **duration** in the context key is not a detail. The
    same ``period_end`` legitimately carries both a three-month and a twelve-month
    figure for a flow concept, and grouping without the duration reports a
    fourth-quarter revenue against a full-year revenue as a 300 % restatement.
    The duration-blind counterfactual is measured on the same pass and published
    in ``load_status``, so the size of the trap is evidence rather than assertion:
    across all 59 indexed tags it inflates the *context* count by ~70 %, while on
    the seven registered concepts here it inflates the event count by well under
    one per cent. The guard is kept regardless — a definition is right or wrong
    independently of how often the wrong one happens to agree.
    """

    def __init__(self, index_path: "str | Path", *,
                 concepts: "tuple[str, ...]" = REVISION_CONCEPTS,
                 materiality: float = REVISION_MATERIALITY) -> None:
        self.index_path = Path(index_path)
        self.concepts = tuple(concepts)
        self.materiality = float(materiality)
        #: cik -> events sorted by observable date
        self._by_cik: "dict[str, list[dict]]" = {}
        self.load_status: dict = {}

    def load(self, ciks: Optional[Iterable[str]] = None) -> dict:
        if not self.index_path.exists():
            self.load_status = {"ok": False, "reason": "CF_INDEX_ABSENT",
                                "path": str(self.index_path)}
            return self.load_status
        wanted = {str(c).zfill(10) for c in (ciks or []) if c} or None
        conn = sqlite3.connect("file:%s?mode=ro" % self.index_path, uri=True)
        conn.row_factory = sqlite3.Row
        contexts = 0
        events = 0
        duration_blind_events = 0
        try:
            cur = conn.execute(
                "SELECT cik, concept_tag, unit, IFNULL(period_start,'') AS ps, "
                "period_end, accession, filed, value, form FROM cf_fact "
                "WHERE concept_tag IN (%s) AND value IS NOT NULL "
                "AND filed IS NOT NULL AND period_end IS NOT NULL "
                "ORDER BY cik, concept_tag, unit, ps, period_end, filed, accession"
                % ",".join("?" * len(self.concepts)), self.concepts)
            group_key = None
            group: "list[sqlite3.Row]" = []
            blind_key = None
            blind: "list[sqlite3.Row]" = []
            for r in cur:
                cik = str(r["cik"]).zfill(10)
                if wanted is not None and cik not in wanted:
                    continue
                key = (cik, r["concept_tag"], r["unit"], r["ps"], r["period_end"])
                if key != group_key:
                    if group:
                        contexts += 1
                        events += self._emit(group_key, group)
                    group_key, group = key, []
                group.append(r)
                # The duration-blind counterfactual, measured on the same pass so
                # the inflation claim is evidence rather than assertion.
                bkey = (cik, r["concept_tag"], r["unit"], r["period_end"])
                if bkey != blind_key:
                    if blind:
                        duration_blind_events += self._count_only(blind)
                    blind_key, blind = bkey, []
                blind.append(r)
            if group:
                contexts += 1
                events += self._emit(group_key, group)
            if blind:
                duration_blind_events += self._count_only(blind)
        finally:
            conn.close()
        for evs in self._by_cik.values():
            evs.sort(key=lambda e: (e["observable_at"], e["concept"]))
        self.load_status = {
            "ok": bool(events), "path": str(self.index_path),
            "concepts": list(self.concepts),
            "materiality_threshold": self.materiality,
            "contexts_scanned": contexts,
            "revision_events": events,
            "issuers_with_revisions": len(self._by_cik),
            "duration_blind_event_count": duration_blind_events,
            "duration_blind_inflation": (
                round(duration_blind_events / events - 1.0, 6) if events else None),
            "context_key": "(cik, concept, unit, period_start, period_end)",
            "event_stamp": "FILED date of the LATER accession",
            "evidence_class": "LEAKAGE_SAFE_PIT_ACCOUNTING_REVISION",
            "contract_version": CONTRACT_VERSION,
        }
        return self.load_status

    def _emit(self, key, group) -> int:
        """Turn one context's fact history into revision events."""
        cik, concept, _unit, _ps, period_end = key
        # One value per accession; the earliest filed date wins for an accession
        # that appears more than once.
        per_accession: "dict[str, tuple[str, float]]" = {}
        for r in group:
            accn = str(r["accession"])
            filed = str(r["filed"])[:10]
            val = float(r["value"])
            prev = per_accession.get(accn)
            if prev is None or filed < prev[0]:
                per_accession[accn] = (filed, val)
        ordered = sorted(((f, a, v) for a, (f, v) in per_accession.items()))
        if len(ordered) < 2:
            return 0
        n = 0
        base_filed, _base_accn, base_val = ordered[0]
        prev_val = base_val
        for filed, accn, val in ordered[1:]:
            scale = abs(val) + abs(prev_val)
            if scale <= 0:
                prev_val = val
                continue
            rel = (val - prev_val) / scale
            if abs(rel) <= self.materiality:
                prev_val = val
                continue      # the number was RESTATED IDENTICALLY: not an event
            self._by_cik.setdefault(cik, []).append({
                "cik": cik, "concept": concept, "period_end": period_end,
                "observable_at": filed, "observable_d": _d(filed),
                "accession": accn,
                "relative_change": rel,
                "abs_relative_change": abs(rel),
                "directional": concept in DIRECTIONAL_CONCEPTS,
                "latency_days": (
                    (_d(filed) - _d(base_filed)).days
                    if _d(filed) and _d(base_filed) else None),
            })
            prev_val = val
            n += 1
        return n

    def _count_only(self, group) -> int:
        per_accession: "dict[str, tuple[str, float]]" = {}
        for r in group:
            accn = str(r["accession"])
            filed = str(r["filed"])[:10]
            prev = per_accession.get(accn)
            if prev is None or filed < prev[0]:
                per_accession[accn] = (filed, float(r["value"]))
        ordered = sorted(((f, a, v) for a, (f, v) in per_accession.items()))
        n = 0
        if len(ordered) < 2:
            return 0
        prev_val = ordered[0][2]
        for _f, _a, val in ordered[1:]:
            scale = abs(val) + abs(prev_val)
            if scale > 0 and abs((val - prev_val) / scale) > self.materiality:
                n += 1
            prev_val = val
        return n

    def events_as_of(self, cik: str, as_of: str) -> "list[dict]":
        evs = self._by_cik.get(str(cik).zfill(10))
        if not evs:
            return []
        cutoff = str(as_of)[:10]
        out = []
        for e in evs:
            if e["observable_at"] > cutoff:
                break
            out.append(e)
        return out

    def observables(self, cik: str, as_of: str) -> Optional[dict]:
        """Primitive revision observables at ``as_of``.

        Returns a fully-zero record for an issuer the index covers but which has
        no revision — "this company has not restated" is a real observation, not
        a missing one. ``None`` is reserved for issuers absent from the index.
        """
        if str(cik).zfill(10) not in self._covered:
            return None
        cutoff = _d(as_of)
        if cutoff is None:
            return None
        evs = self.events_as_of(cik, as_of)
        w1 = cutoff - timedelta(days=WINDOW_1Y)
        w3 = cutoff - timedelta(days=WINDOW_3Y)
        e1 = [e for e in evs if e["observable_d"] and e["observable_d"] > w1]
        e3 = [e for e in evs if e["observable_d"] and e["observable_d"] > w3]
        directional = [e for e in e1 if e["directional"]]
        years3 = {e["observable_at"][:4] for e in e3}
        return {
            "revision_count_1y": float(len(e1)),
            "revision_count_3y": float(len(e3)),
            "revision_recent_1y": 1.0 if e1 else 0.0,
            "revision_max_magnitude_1y": (
                max(e["abs_relative_change"] for e in e1) if e1 else 0.0),
            "revision_concept_breadth_1y": float(
                len({e["concept"] for e in e1})),
            "revision_years_active_3y": float(len(years3)),
            "revision_directional_1y": (
                _mean([e["relative_change"] for e in directional])
                if directional else 0.0),
            "revision_latency_days": (
                _median([e["latency_days"] for e in e1]) if e1 else None),
        }

    @property
    def _covered(self) -> set:
        cached = getattr(self, "_covered_cache", None)
        if cached is None:
            cached = self._issuer_universe()
            self._covered_cache = cached
        return cached

    def _issuer_universe(self) -> set:
        """Every CIK the index carries for the registered concepts — including
        the ones that never restated, which must score 0 rather than missing."""
        if not self.index_path.exists():
            return set()
        conn = sqlite3.connect("file:%s?mode=ro" % self.index_path, uri=True)
        try:
            rows = conn.execute(
                "SELECT DISTINCT cik FROM cf_fact WHERE concept_tag IN (%s)"
                % ",".join("?" * len(self.concepts)), self.concepts).fetchall()
        finally:
            conn.close()
        return {str(r[0]).zfill(10) for r in rows}


# =========================================================================== #
# 3. Share dynamics — split-normalised net issuance.
# =========================================================================== #
#: A share change beyond this magnitude in one year is treated as a capital-
#: structure event the capital-factor carry did not capture (a reverse merger, a
#: share-class re-registration), not as ordinary issuance. Such rows are dropped
#: rather than winsorized into the cross-section, exactly as the released market
#: equity owner drops an implausible value.
MAX_PLAUSIBLE_ANNUAL_SHARE_CHANGE = 3.0

#: Persistent dilution needs a threshold that is above reporting noise. One per
#: cent of shares a year is the conventional line and is fixed here.
DILUTION_YEAR_THRESHOLD = 0.01


class ShareDynamicsHistory:
    """Point-in-time net share issuance, composed from the RELEASED owners.

    Stage 26 built the point-in-time share index and the unadjusted price
    surface; this class adds no reader of its own. What it adds is the
    normalisation that makes two share counts from two different filings
    comparable:

        n(x) = shares(report x) / f(filed x),   f(t) = close_capital(t)/close_none(t)

    ``f`` is the cumulative capital-event factor, so dividing by it expresses
    every count in the same pre-split units. A 2-for-1 split doubles both the
    reported count and ``f`` and therefore cancels — which is the entire point:
    **a split is not issuance.** What survives is real dilution and real buyback.
    """

    def __init__(self, shares, prices) -> None:
        self.shares = shares
        self.prices = prices
        self._norm_cache: dict = {}

    def normalised_shares(self, *, symbol: str, cik: str,
                          as_of: str) -> Optional[dict]:
        """Split-normalised share count from the latest filing on or before
        ``as_of``."""
        key = (symbol, str(cik), str(as_of)[:10])
        if key in self._norm_cache:
            return self._norm_cache[key]
        rec = self.shares.shares_as_of(cik, as_of)
        out = None
        if rec and rec.get("filed"):
            px = self.prices.closes_as_of(symbol, rec["filed"])
            if px and px.get("capital_factor"):
                f = float(px["capital_factor"])
                if f > 0:
                    out = {"normalised": float(rec["shares"]) / f,
                           "shares": float(rec["shares"]),
                           "capital_factor": f,
                           "filed": rec["filed"],
                           "concept": rec.get("concept"),
                           "age_days": rec.get("age_days")}
        self._norm_cache[key] = out
        return out

    def observables(self, *, symbol: str, cik: str,
                    as_of: str) -> Optional[dict]:
        """Primitive share-dynamics observables at ``as_of``."""
        cutoff = _d(as_of)
        if cutoff is None:
            return None
        pts = {}
        for label, back in (("t0", 0), ("t1", WINDOW_1Y), ("t2", WINDOW_2Y),
                            ("t3", WINDOW_3Y)):
            d = (cutoff - timedelta(days=back)).isoformat()
            pts[label] = self.normalised_shares(symbol=symbol, cik=cik, as_of=d)
        if pts["t0"] is None:
            return None

        def _chg(a, b):
            """Growth in split-normalised share count from b to a."""
            if a is None or b is None:
                return None
            if b["normalised"] <= 0:
                return None
            # Two reads that resolved to the SAME filing carry no information
            # about issuance; reporting them as 0 % would be a fabricated
            # observation of stability.
            if a.get("filed") == b.get("filed"):
                return None
            g = a["normalised"] / b["normalised"] - 1.0
            if abs(g) > MAX_PLAUSIBLE_ANNUAL_SHARE_CHANGE:
                return None
            return g

        iss1 = _chg(pts["t0"], pts["t1"])
        iss2 = _chg(pts["t0"], pts["t2"])
        prior1 = _chg(pts["t1"], pts["t2"])
        prior2 = _chg(pts["t2"], pts["t3"])
        years = [x for x in (iss1, prior1, prior2) if x is not None]
        return {
            "net_issuance_1y": iss1,
            "net_issuance_2y": iss2,
            "net_issuance_prior_1y": prior1,
            "issuance_acceleration": _sub(iss1, prior1),
            "buyback_years_3y": (
                float(sum(1 for g in years if g < -DILUTION_YEAR_THRESHOLD))
                if len(years) >= 2 else None),
            "dilution_years_3y": (
                float(sum(1 for g in years if g > DILUTION_YEAR_THRESHOLD))
                if len(years) >= 2 else None),
            # The RAW reported count at the latest filing on or before the
            # formation date. Insider share volumes are reported in traded units,
            # so they must be scaled by this rather than by the split-normalised
            # count used for the issuance ratios.
            "shares_outstanding": pts["t0"]["shares"],
            "share_observations": float(sum(1 for v in pts.values() if v)),
            "share_concept": pts["t0"].get("concept"),
            "share_age_days": pts["t0"].get("age_days"),
        }

    def contract(self) -> dict:
        return {
            "contract_version": CONTRACT_VERSION,
            "business_concept": "split-normalised net share issuance, PIT",
            "normalisation": "n(x) = shares(report x) / f(filed x), "
                             "f(t) = close_capital(t)/close_none(t)",
            "why": "a stock split changes the reported count and the capital "
                   "factor by the SAME multiple, so it cancels; what survives is "
                   "economic issuance and buyback",
            "explicitly_not": [
                "a share count read from today's snapshot",
                "a raw count difference across a split",
                "the cash payout ratio, which is a different economic quantity",
            ],
            "point_in_time_rule": "each endpoint uses only counts FILED on or "
                                  "before that endpoint's own date",
            "same_filing_policy": "two endpoints resolving to the SAME filing "
                                  "yield None, never 0 %",
            "share_source": getattr(self.shares, "load_status", {}),
            "price_source": getattr(self.prices, "load_status", {}),
            "max_plausible_annual_change": MAX_PLAUSIBLE_ANNUAL_SHARE_CHANGE,
            "dilution_year_threshold": DILUTION_YEAR_THRESHOLD,
        }


# =========================================================================== #
# 4. Insider transactions — Forms 3/4/5, free and first-party.
# =========================================================================== #
#: Only OPEN-MARKET trades carry an opinion. A grant (``A``), an option exercise
#: (``M``), a tax-withholding disposition (``F``) and a gift (``G``) are all
#: compensation mechanics the insider did not choose the timing of, and mixing
#: them in is how an insider signal becomes a payroll calendar.
BUY_CODE = "P"
SELL_CODE = "S"
OPEN_MARKET_CODES = frozenset({BUY_CODE, SELL_CODE})

#: Acquisition/disposition flag, used to confirm the transaction code rather
#: than to replace it — a ``P`` marked ``D`` is a data error and is dropped.
ACQUIRED = "A"
DISPOSED = "D"

#: Role strings in ``RPTOWNER_RELATIONSHIP`` that mean the reporter runs the
#: company, as opposed to a passive ten-per-cent holder.
INSIDER_ROLE_TOKENS = ("OFFICER", "DIRECTOR")

INSIDER_WINDOW_DAYS = 182
INSIDER_CLUSTER_MIN_BUYERS = 3

_MONTHS = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
           "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}


def parse_sec_date(value) -> Optional[date]:
    """SEC structured-data date, ``DD-MON-YYYY``, to a date."""
    s = str(value or "").strip().upper()
    if not s:
        return None
    parts = s.split("-")
    if len(parts) == 3 and parts[1] in _MONTHS:
        try:
            return date(int(parts[2]), _MONTHS[parts[1]], int(parts[0]))
        except ValueError:
            return None
    return _d(s)


class InsiderTransactionHistory:
    """Open-market insider trades, point-in-time by Form 4 FILING date.

    The point-in-time rule here is the one that decides whether the family is
    admissible at all. A Form 4 reports a transaction that already happened — up
    to two business days earlier, and historically much longer — so keying the
    signal on ``TRANS_DATE`` would let the research see a trade before the market
    could. Every event is stamped at ``FILING_DATE``, which is the first moment
    an outside observer could have acted on it.

    Only ``NONDERIV_TRANS`` rows with transaction code ``P`` or ``S`` are kept:
    those are open-market purchases and sales, the only rows where the insider
    chose both the direction and the timing.
    """

    def __init__(self, cache_root: "str | Path") -> None:
        self.cache_root = Path(cache_root)
        #: cik -> [(filed_iso, owner_int, is_officer, sign, shares, dollars)]
        self._by_cik: "dict[str, list[tuple]]" = {}
        self.load_status: dict = {}
        self._quarters: "list[str]" = []

    def load(self, ciks: Optional[Iterable[str]] = None) -> dict:
        from . import sec_financial_statement_sets as _fsds

        if not self.cache_root.exists():
            self.load_status = {"ok": False, "reason": "INSIDER_CACHE_ABSENT",
                                "path": str(self.cache_root)}
            return self.load_status
        wanted = {str(int(str(c).lstrip("0") or "0")) for c in (ciks or []) if c} \
            or None
        quarters: "list[str]" = []
        subs_seen = trans_seen = kept = 0
        dropped_code = dropped_flag = 0
        owners_seen = 0
        for qdir in sorted(p for p in self.cache_root.iterdir() if p.is_dir()):
            sub_p = qdir / "SUBMISSION.tsv"
            trans_p = qdir / "NONDERIV_TRANS.tsv"
            own_p = qdir / "REPORTINGOWNER.tsv"
            if not (sub_p.exists() and trans_p.exists() and own_p.exists()):
                continue
            quarters.append(qdir.name)

            accn_meta: "dict[str, tuple[str, str]]" = {}
            for r in _fsds.parse_tsv(
                    sub_p.read_bytes(),
                    required=("ACCESSION_NUMBER", "FILING_DATE", "ISSUERCIK",
                              "DOCUMENT_TYPE")):
                subs_seen += 1
                if str(r.get("DOCUMENT_TYPE") or "").strip() not in ("4", "5"):
                    continue
                raw = "".join(ch for ch in str(r.get("ISSUERCIK") or "")
                              if ch.isdigit())
                if not raw:
                    continue
                cik = str(int(raw))
                if wanted is not None and cik not in wanted:
                    continue
                filed = parse_sec_date(r.get("FILING_DATE"))
                if filed is None:
                    continue
                accn_meta[r["ACCESSION_NUMBER"]] = (cik, filed.isoformat())
            if not accn_meta:
                continue

            owner: "dict[str, tuple[int, bool]]" = {}
            for r in _fsds.parse_tsv(
                    own_p.read_bytes(),
                    required=("ACCESSION_NUMBER", "RPTOWNERCIK",
                              "RPTOWNER_RELATIONSHIP")):
                accn = r.get("ACCESSION_NUMBER")
                if accn not in accn_meta:
                    continue
                owners_seen += 1
                raw = "".join(ch for ch in str(r.get("RPTOWNERCIK") or "")
                              if ch.isdigit())
                rel = str(r.get("RPTOWNER_RELATIONSHIP") or "").upper()
                owner[accn] = (int(raw or 0),
                               any(tok in rel for tok in INSIDER_ROLE_TOKENS))

            for r in _fsds.parse_tsv(
                    trans_p.read_bytes(),
                    required=("ACCESSION_NUMBER", "TRANS_CODE", "TRANS_SHARES",
                              "TRANS_ACQUIRED_DISP_CD")):
                trans_seen += 1
                accn = r.get("ACCESSION_NUMBER")
                meta = accn_meta.get(accn)
                if meta is None:
                    continue
                code = str(r.get("TRANS_CODE") or "").strip().upper()
                if code not in OPEN_MARKET_CODES:
                    dropped_code += 1
                    continue
                flag = str(r.get("TRANS_ACQUIRED_DISP_CD") or "").strip().upper()
                expected = ACQUIRED if code == BUY_CODE else DISPOSED
                if flag and flag != expected:
                    dropped_flag += 1
                    continue
                try:
                    shares = float(r.get("TRANS_SHARES") or 0.0)
                except ValueError:
                    continue
                if shares <= 0:
                    continue
                try:
                    price = float(r.get("TRANS_PRICEPERSHARE") or 0.0)
                except ValueError:
                    price = 0.0
                cik, filed = meta
                own_cik, is_officer = owner.get(accn, (0, False))
                sign = 1 if code == BUY_CODE else -1
                self._by_cik.setdefault(cik, []).append(
                    (filed, own_cik, is_officer, sign, shares,
                     shares * price if price > 0 else 0.0))
                kept += 1
        for evs in self._by_cik.values():
            evs.sort()
        self._quarters = quarters
        self.load_status = {
            "ok": bool(kept), "path": str(self.cache_root),
            "quarters_loaded": len(quarters),
            "first_quarter": quarters[0] if quarters else None,
            "last_quarter": quarters[-1] if quarters else None,
            "submissions_scanned": subs_seen,
            "reporting_owner_rows": owners_seen,
            "transaction_rows_scanned": trans_seen,
            "open_market_events_kept": kept,
            "dropped_non_open_market_code": dropped_code,
            "dropped_direction_flag_mismatch": dropped_flag,
            "issuers_with_events": len(self._by_cik),
            "codes_kept": sorted(OPEN_MARKET_CODES),
            "event_stamp": "Form 4/5 FILING_DATE (never TRANS_DATE)",
            "evidence_class": "LEAKAGE_SAFE_PIT_SEC_INSIDER_FORM345",
            "contract_version": CONTRACT_VERSION,
        }
        return self.load_status

    def events_as_of(self, cik: str, as_of: str, *,
                     window_days: int = INSIDER_WINDOW_DAYS) -> "list[tuple]":
        evs = self._by_cik.get(str(int(str(cik).lstrip("0") or "0")))
        if not evs:
            return []
        cutoff = _d(as_of)
        if cutoff is None:
            return []
        lo = (cutoff - timedelta(days=window_days)).isoformat()
        hi = cutoff.isoformat()
        return [e for e in evs if lo < e[0] <= hi]

    def covered_ciks(self) -> set:
        return set(self._by_cik)

    def observables(self, *, cik: str, as_of: str,
                    shares_outstanding: Optional[float] = None,
                    market_equity: Optional[float] = None,
                    window_days: int = INSIDER_WINDOW_DAYS) -> Optional[dict]:
        """Primitive insider observables at ``as_of``.

        An issuer the archive covers but with no open-market trade in the window
        scores zero, not missing: "no insider bought or sold" is an observation.
        An issuer absent from the archive entirely returns ``None``.
        """
        key = str(int(str(cik).lstrip("0") or "0"))
        if key not in self._by_cik:
            return None
        evs = self.events_as_of(cik, as_of, window_days=window_days)
        buy_sh = sum(e[4] for e in evs if e[3] > 0)
        sell_sh = sum(e[4] for e in evs if e[3] < 0)
        buy_usd = sum(e[5] for e in evs if e[3] > 0)
        sell_usd = sum(e[5] for e in evs if e[3] < 0)
        off_buy = sum(e[4] for e in evs if e[3] > 0 and e[2])
        off_sell = sum(e[4] for e in evs if e[3] < 0 and e[2])
        buyers = {e[1] for e in evs if e[3] > 0 and e[1]}
        sellers = {e[1] for e in evs if e[3] < 0 and e[1]}
        traders = buyers | sellers
        so = float(shares_outstanding) if shares_outstanding else None
        me = float(market_equity) if market_equity else None
        return {
            "insider_events_6m": float(len(evs)),
            "net_buy_share_fraction_6m": (
                (buy_sh - sell_sh) / so if so and so > 0 else None),
            "officer_net_buy_share_fraction_6m": (
                (off_buy - off_sell) / so if so and so > 0 else None),
            "net_buy_dollar_to_market_equity_6m": (
                (buy_usd - sell_usd) / me if me and me > 0 else None),
            "sell_dollar_to_market_equity_6m": (
                sell_usd / me if me and me > 0 else None),
            # No insider traded at all: that is an ABSENCE of information about
            # direction, not a cross-section of zero buyers. Returning 0.0 would
            # pool the quiet companies with the all-sellers.
            "buyer_ratio_6m": (len(buyers) / len(traders)) if traders else None,
            "cluster_buy_6m": 1.0 if len(buyers) >= INSIDER_CLUSTER_MIN_BUYERS
                              else 0.0,
            "distinct_buyers_6m": float(len(buyers)),
            "distinct_sellers_6m": float(len(sellers)),
        }

    def parse_validation(self) -> dict:
        """An external check that the transaction codes and dates parsed right.

        A returns study cannot validate its own inputs, so this uses a fact that
        is true independently of anything being tested: corporate insiders buy
        into crashes. If the direction flags, transaction codes or filing dates
        were mis-parsed, the buy share would be flat or noisy across time. If
        they are right, it must spike at the two great bottoms in the sample.
        """
        by_q: "dict[str, list]" = {}
        for evs in self._by_cik.values():
            for filed, _own, _off, sign, _sh, _usd in evs:
                key = "%sQ%d" % (filed[:4], (int(filed[5:7]) - 1) // 3 + 1)
                cell = by_q.setdefault(key, [0, 0])
                cell[0 if sign > 0 else 1] += 1
        shares = {k: v[0] / max(1, v[0] + v[1]) for k, v in by_q.items()}
        buys = sum(v[0] for v in by_q.values())
        sells = sum(v[1] for v in by_q.values())
        top = sorted(shares.items(), key=lambda kv: -kv[1])[:3]
        return {
            "check": "insider buy share of open-market trades, by quarter",
            "overall_buy_share": round(buys / max(1, buys + sells), 6),
            "total_open_market_buys": buys,
            "total_open_market_sells": sells,
            "highest_buy_share_quarters": [
                {"quarter": q, "buy_share": round(s, 4)} for q, s in top],
            "expectation": "the peaks should land on equity-market bottoms; "
                           "insiders buy into crashes",
            "quarterly_buy_share": {k: round(v, 4)
                                    for k, v in sorted(shares.items())},
        }

    def acquisition_manifest(self) -> dict:
        return {
            "contract_id": "release27_insider_source_manifest/1",
            "reader_contract": CONTRACT_VERSION,
            "source": "SEC Insider Transactions Data Sets (Forms 3/4/5, DERA)",
            "acquired_by": "scripts/acquire_sec_insider_transactions.py via "
                           "alpha_agent.sec_financial_statement_sets."
                           "QuarterlyDataSetAcquirer",
            "acquired_in_this_campaign": True,
            "cache_root": str(self.cache_root),
            "quarters": self._quarters,
            "quarters_count": len(self._quarters),
            "members_transferred": ["SUBMISSION.tsv", "REPORTINGOWNER.tsv",
                                    "NONDERIV_TRANS.tsv"],
            "license_note": "US federal government work; SEC EDGAR public data, "
                            "free for research use under SEC fair-access rules",
            "cost": "USD 0.00 - no vendor, no key, no quota",
        }


# =========================================================================== #
# 5. The complete filing stream — EDGAR quarterly full index.
# =========================================================================== #
#: Form-type groups this module recognises. Each is an economically distinct
#: corporate event, and the set is fixed here before any result exists rather
#: than discovered by scanning which form happened to predict returns.
STREAM_LATE_NOTIFICATION = ("NT 10-K", "NT 10-Q", "NT 10-K/A", "NT 10-Q/A")
STREAM_SHELF_OFFERING = ("S-3", "S-3/A", "S-3ASR", "S-1", "S-1/A",
                         "424B1", "424B2", "424B3", "424B4", "424B5", "424B7")
STREAM_ACTIVIST = ("SC 13D", "SC 13D/A")
STREAM_CURRENT_REPORT = ("8-K", "8-K/A")

_STREAM_GROUPS = {
    "late_notification": frozenset(STREAM_LATE_NOTIFICATION),
    "shelf_offering": frozenset(STREAM_SHELF_OFFERING),
    "activist": frozenset(STREAM_ACTIVIST),
    "current_report": frozenset(STREAM_CURRENT_REPORT),
}


class EdgarFilingStreamHistory:
    """Every filing an issuer made, of every form type, point-in-time.

    ``sub.txt`` only ever sees a submission that carried XBRL financial
    statements, which is why the filing-behaviour family above can measure how
    LATE a 10-K was but not whether the company formally told the SEC it would
    BE late. The quarterly full index closes that: it lists every filing of every
    form by CIK and filing date, so notification-of-late-filing, shelf
    registrations, activist stake disclosures and unscheduled current reports all
    become observable.

    Only the four registered form groups are retained. Keeping the whole index
    in memory would be both wasteful and an invitation to scan for whichever
    form happens to correlate with returns, which is the search this programme
    exists not to do.
    """

    def __init__(self, cache_root: "str | Path") -> None:
        self.cache_root = Path(cache_root)
        #: cik -> [(filed_iso, group, form)]
        self._by_cik: "dict[str, list[tuple]]" = {}
        self.load_status: dict = {}
        self._quarters: "list[str]" = []

    def load(self, ciks: Optional[Iterable[str]] = None) -> dict:
        if not self.cache_root.exists():
            self.load_status = {"ok": False, "reason": "FULL_INDEX_CACHE_ABSENT",
                                "path": str(self.cache_root)}
            return self.load_status
        wanted = {str(int(str(c).lstrip("0") or "0")) for c in (ciks or []) if c} \
            or None
        by_form = {}
        for group, forms in _STREAM_GROUPS.items():
            for f in forms:
                by_form[f] = group
        quarters: "list[str]" = []
        scanned = kept = 0
        for qdir in sorted(p for p in self.cache_root.iterdir() if p.is_dir()):
            member = qdir / "master.idx"
            if not member.exists():
                continue
            quarters.append(qdir.name)
            text = member.read_bytes().decode("latin-1")
            for line in text.split("\n"):
                if line.count("|") != 4:
                    continue
                cik, _name, form, filed, _path = line.split("|", 4)
                scanned += 1
                group = by_form.get(form.strip().upper())
                if group is None:
                    continue
                cik = cik.strip()
                if not cik.isdigit():
                    continue
                cik = str(int(cik))
                if wanted is not None and cik not in wanted:
                    continue
                d = _d(filed.strip())
                if d is None:
                    continue
                self._by_cik.setdefault(cik, []).append(
                    (d.isoformat(), group, form.strip().upper()))
                kept += 1
        for evs in self._by_cik.values():
            evs.sort()
        self._quarters = quarters
        counts: "dict[str, int]" = {}
        for evs in self._by_cik.values():
            for _f, g, _form in evs:
                counts[g] = counts.get(g, 0) + 1
        self.load_status = {
            "ok": bool(kept), "path": str(self.cache_root),
            "quarters_loaded": len(quarters),
            "first_quarter": quarters[0] if quarters else None,
            "last_quarter": quarters[-1] if quarters else None,
            "index_rows_scanned": scanned,
            "events_kept": kept,
            "issuers_with_events": len(self._by_cik),
            "events_by_group": counts,
            "registered_form_groups": {k: sorted(v)
                                       for k, v in _STREAM_GROUPS.items()},
            "event_stamp": "EDGAR Date Filed",
            "evidence_class": "LEAKAGE_SAFE_PIT_SEC_FILING_STREAM",
            "contract_version": CONTRACT_VERSION,
        }
        return self.load_status

    def covered_ciks(self) -> set:
        return set(self._by_cik)

    def observables(self, cik: str, as_of: str) -> Optional[dict]:
        key = str(int(str(cik).lstrip("0") or "0"))
        evs = self._by_cik.get(key)
        if evs is None:
            return None
        cutoff = _d(as_of)
        if cutoff is None:
            return None
        hi = cutoff.isoformat()
        w1 = (cutoff - timedelta(days=WINDOW_1Y)).isoformat()
        w4 = (cutoff - timedelta(days=WINDOW_3Y + WINDOW_1Y)).isoformat()
        e1 = [e for e in evs if w1 < e[0] <= hi]
        prior3 = [e for e in evs if w4 < e[0] <= w1]

        def _n(bucket, group):
            return float(sum(1 for e in bucket if e[1] == group))

        cur_8k = _n(e1, "current_report")
        prior_8k_annual = _n(prior3, "current_report") / 3.0
        return {
            "late_notification_1y": 1.0 if _n(e1, "late_notification") else 0.0,
            "shelf_offering_1y": 1.0 if _n(e1, "shelf_offering") else 0.0,
            "activist_stake_1y": 1.0 if _n(e1, "activist") else 0.0,
            "current_report_count_1y": cur_8k,
            "abnormal_current_report_1y": (
                cur_8k - prior_8k_annual if prior3 else None),
            "form_breadth_1y": float(len({e[2] for e in e1})),
        }

    def acquisition_manifest(self) -> dict:
        return {
            "contract_id": "release27_filing_stream_source_manifest/1",
            "reader_contract": CONTRACT_VERSION,
            "source": "SEC EDGAR quarterly full index (master.idx)",
            "acquired_by": "scripts/acquire_sec_quarterly_dataset.py "
                           "--dataset edgar_full_index",
            "acquired_in_this_campaign": True,
            "cache_root": str(self.cache_root),
            "quarters": self._quarters,
            "quarters_count": len(self._quarters),
            "license_note": "US federal government work; SEC EDGAR public data, "
                            "free for research use under SEC fair-access rules",
            "cost": "USD 0.00 - no vendor, no key, no quota",
        }


__all__ = [
    "CONTRACT_VERSION", "ANNUAL_FORMS", "QUARTERLY_FORMS", "PERIODIC_FORMS",
    "BUY_CODE", "SELL_CODE", "OPEN_MARKET_CODES", "INSIDER_ROLE_TOKENS",
    "STREAM_LATE_NOTIFICATION", "STREAM_SHELF_OFFERING", "STREAM_ACTIVIST",
    "STREAM_CURRENT_REPORT", "EdgarFilingStreamHistory",
    "INSIDER_WINDOW_DAYS", "INSIDER_CLUSTER_MIN_BUYERS", "parse_sec_date",
    "InsiderTransactionHistory",
    "AMENDMENT_FORMS", "TRANSITION_FORMS", "DEADLINE_DAYS", "DEFAULT_DEADLINE",
    "DEADLINE_GRACE_DAYS", "MARKET_CLOSE_HOUR_ET", "WINDOW_1Y", "WINDOW_2Y",
    "WINDOW_3Y", "MIN_OWN_HISTORY_FILINGS", "EXPECTED_CADENCE_DAYS",
    "REVISION_CONCEPTS", "REVISION_MATERIALITY", "DIRECTIONAL_CONCEPTS",
    "MAX_PLAUSIBLE_ANNUAL_SHARE_CHANGE", "DILUTION_YEAR_THRESHOLD",
    "FilingHistory", "FactRevisionHistory", "ShareDynamicsHistory",
]
