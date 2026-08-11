"""tests/test_stage15b_alfred_backfill.py — Stage 15B ALFRED historical vintage
backfill capability (config-gated extension of the fred_alfred collector).

Hermetic: a fake transport models an ALFRED endpoint (2000-vintage-date cap,
per-series archive start, clamped carry-ins, a genuine revision). NEVER calls a
network. Verifies the historical-backfill mode:
  * walks the realtime axis in bounded chunks (chunking / "pagination");
  * preserves the TRUE realtime_start as available_at (PIT vintage);
  * retains multiple vintages of a revised observation, never substituting the
    latest value for a historical one;
  * drops ALFRED carry-ins clamped to a chunk start (no false availability);
  * bisects a chunk that trips the vintage-date cap and still collects it;
  * skips a pre-archive realtime window ("does not exist in ALFRED") without
    a hard error;
  * is resumable (completed chunks in the cursor are not re-requested);
  * is idempotent (identical record_ids across runs);
  * leaves the DEFAULT (no historical_backfill) rolling-window path unchanged.
"""
from __future__ import annotations

import json
import sys
import urllib.parse
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.alpha_agent.collectors.base import CollectorContext, RawArchive  # noqa: E402
from paper_trader.alpha_agent.collectors.fred_alfred import FredAlfredCollector  # noqa: E402

FRED_KEY = "TESTFREDKEY1234567890"
AS_OF = "2021-06-01"
ARCHIVE = "2018-01-01"          # TESTDLY has NO ALFRED vintages before this
REVISED_OBS = "2020-05-15"      # obs with two genuine vintages
V1, V2 = "1.11", "2.22"         # first release vs revision values


def _obs_dates():
    out = []
    for y in range(2016, 2022):
        for m in range(1, 13):
            d = "%04d-%02d-15" % (y, m)
            if d <= AS_OF:
                out.append(d)
    return out


ALL_OBS = _obs_dates()


def _gen_rows(sid, obs_start, a, b, archive_start):
    """Emulate ALFRED output_type=1 (observations by realtime period) for [a,b]."""
    rows = []
    for od in ALL_OBS:
        if od < obs_start:
            continue
        periods = []
        if sid == "TESTDLY" and od == REVISED_OBS:
            # two realtime periods: first release, then a revision
            periods = [(max(od, archive_start), "2020-09-14", V1),
                       ("2020-09-15", "9999-12-31", V2)]
        else:
            periods = [(max(od, archive_start), "9999-12-31", "9.99")]
        for eff_rel, rte, val in periods:
            if eff_rel > b:
                continue
            rts = a if eff_rel <= a else eff_rel  # clamp carry-ins to window start
            rows.append({"date": od, "realtime_start": rts,
                         "realtime_end": rte, "value": val})
    return rows


class FakeALFRED:
    def __init__(self):
        self.calls = []

    def __call__(self, request, timeout):
        url = request["url"]
        self.calls.append(url)
        q = {k: v[0] for k, v in
             urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).items()}
        sid, a, b, os_ = (q["series_id"], q["realtime_start"],
                          q["realtime_end"], q["observation_start"])

        def r200(rows):
            body = json.dumps({"realtime_start": a, "realtime_end": b,
                               "count": len(rows), "observations": rows}).encode()
            return {"status": 200, "headers": {}, "body": body, "error": None}

        def r400(msg):
            body = json.dumps({"error_code": 400, "error_message": msg}).encode()
            return {"status": 400, "headers": {}, "body": body, "error": None}

        if sid == "TESTCAP":
            # trips the vintage cap for any window spanning more than one year
            if a[:4] != b[:4]:
                return r400("Bad Request.  There are 9999 vintage dates in the "
                            "specified real-time period: %s to %s.  This exceeds the "
                            "maximum number of vintage dates allowed for this file "
                            "type (2000)." % (a, b))
            return r200(_gen_rows(sid, os_, a, b, "2016-01-01"))
        # TESTDLY: pre-archive realtime windows do not exist in ALFRED
        if b < ARCHIVE:
            return r400("Bad Request.  The series does not exist in ALFRED but may "
                        "exist in FRED.  Try setting realtime_start and realtime_end "
                        "to today's date or removing the variables.")
        return r200(_gen_rows(sid, os_, a, b, ARCHIVE))


def _ctx(tmp_path, transport, *, historical=True, checkpoint=None):
    scfg = {
        "enabled": True, "base_url": "https://api.stlouisfed.org/fred",
        "allowed_env_vars": ["FRED_API_KEY"], "min_interval_seconds": 0.0,
        "use_alfred_vintages": True, "observation_window_days": 45,
        "series_allowlist": [
            {"series_id": "TESTDLY", "macro_family": "interest_rates", "title": "daily"},
            {"series_id": "TESTCAP", "macro_family": "interest_rates", "title": "dense"},
        ],
    }
    if historical:
        scfg["historical_backfill"] = {
            "observation_start": "2016-01-01", "realtime_chunk_years": 2,
            "min_chunk_years": 1, "max_requests_per_series": 60,
        }
    config = {
        "limits": {"max_retries": 1, "backoff_base_seconds": 0.0,
                   "backoff_multiplier": 1.0, "http_timeout_seconds": 5,
                   "raw_object_max_bytes": 1 << 20, "circuit_breaker_threshold": 5},
        "secret_redaction": {"redacted_query_params": ["api_key"],
                             "redaction_placeholder": "REDACTED"},
    }
    archive = RawArchive(tmp_path / "raw", tmp_path, set(), 1 << 20)
    return CollectorContext(
        config=config, source_cfg=scfg, archive=archive, transport=transport,
        now_iso=lambda: "2026-08-11T00:00:00", clock=lambda: 0.0,
        sleep=lambda s: None, secrets=[FRED_KEY], user_agent="ua/1",
        checkpoint=(checkpoint or {}), env={"FRED_API_KEY": FRED_KEY})


def _records(coll):
    return [r for r in coll.records]


def _by_series(recs, sid):
    return [r for r in recs if r["normalized_payload"]["series_id"] == sid]


def test_backfill_chunks_and_preserves_true_vintages(tmp_path):
    t = FakeALFRED()
    coll = FredAlfredCollector(_ctx(tmp_path, t))
    coll.collect(AS_OF)
    recs = _records(coll)
    dly = _by_series(recs, "TESTDLY")
    assert dly, "historical backfill produced TESTDLY records"
    # available_at is ALFRED's realtime_start on EVERY record (never fabricated)
    for r in dly:
        assert r["available_at"] == r["normalized_payload"]["realtime_start"]
        assert r["normalized_payload"]["point_in_time_vintage"] is True
        assert r["normalized_payload"]["historical_backfill"] is True
    # chunking actually happened: >1 realtime window requested for TESTDLY
    windows = {urllib.parse.parse_qs(urllib.parse.urlsplit(u).query)["realtime_start"][0]
               for u in t.calls if "series_id=TESTDLY" in u}
    assert len(windows) >= 2


def test_backfill_drops_clamped_carry_ins(tmp_path):
    coll = FredAlfredCollector(_ctx(tmp_path, FakeALFRED()))
    coll.collect(AS_OF)
    dly = _by_series(_records(coll), "TESTDLY")
    # no stored record carries availability equal to a non-first chunk boundary
    avails = {r["available_at"] for r in dly}
    assert "2018-01-01" not in avails      # chunk-2 start (clamped carry-ins)
    assert "2020-01-01" not in avails      # chunk-3 start
    # nothing before the ALFRED archive survives as a genuine vintage
    assert min(avails) >= ARCHIVE
    # no stored record is flagged clamped (all kept ones are genuine releases)
    assert all(r["normalized_payload"]["availability_clamped_to_window"] is False
               for r in dly)


def test_backfill_retains_multiple_vintages_no_substitution(tmp_path):
    coll = FredAlfredCollector(_ctx(tmp_path, FakeALFRED()))
    coll.collect(AS_OF)
    revs = [r for r in _by_series(_records(coll), "TESTDLY")
            if r["normalized_payload"]["observation_date"] == REVISED_OBS]
    avails = sorted(r["available_at"] for r in revs)
    values = {r["normalized_payload"]["value"] for r in revs}
    assert avails == ["2020-05-15", "2020-09-15"]       # both vintages retained
    assert values == {V1, V2}                            # historical value preserved
    assert V1 != V2                                       # not overwritten by latest


def test_backfill_bisects_on_vintage_cap(tmp_path):
    t = FakeALFRED()
    coll = FredAlfredCollector(_ctx(tmp_path, t))
    coll.collect(AS_OF)
    cap = _by_series(_records(coll), "TESTCAP")
    assert cap, "cap-tripping series still collected after bisection"
    # the 2-year chunk [2016..2017] was split into single-year windows
    cap_windows = {(urllib.parse.parse_qs(urllib.parse.urlsplit(u).query)["realtime_start"][0],
                    urllib.parse.parse_qs(urllib.parse.urlsplit(u).query)["realtime_end"][0])
                   for u in t.calls if "series_id=TESTCAP" in u}
    assert ("2016-01-01", "2016-12-31") in cap_windows
    assert ("2017-01-01", "2017-12-31") in cap_windows


def test_backfill_skips_pre_archive_without_hard_error(tmp_path):
    coll = FredAlfredCollector(_ctx(tmp_path, FakeALFRED()))
    coll.collect(AS_OF)
    # pre-archive 400 ("does not exist in ALFRED") is not a hard error
    assert not any(e["error_type"] == "HIST_BAD_REQUEST" for e in coll.errors)
    # and it produced no pre-archive TESTDLY observations
    dly = _by_series(_records(coll), "TESTDLY")
    assert all(r["normalized_payload"]["observation_date"] >= "2017-01-01" for r in dly)


def test_backfill_resume_skips_completed_chunks(tmp_path):
    t = FakeALFRED()
    completed = ["TESTDLY|2018-01-01|2019-12-31"]
    ck = {"cursor": {"historical_completed_chunks": completed}}
    coll = FredAlfredCollector(_ctx(tmp_path, t, checkpoint=ck))
    coll.collect(AS_OF)
    # the completed chunk's realtime window is never re-requested for TESTDLY
    for u in t.calls:
        if "series_id=TESTDLY" in u:
            q = urllib.parse.parse_qs(urllib.parse.urlsplit(u).query)
            assert not (q["realtime_start"][0] == "2018-01-01"
                        and q["realtime_end"][0] == "2019-12-31")
    # cursor still records the chunk as completed
    assert "TESTDLY|2018-01-01|2019-12-31" in coll.cursor["historical_completed_chunks"]


def test_backfill_is_idempotent(tmp_path):
    a = FredAlfredCollector(_ctx(tmp_path / "a", FakeALFRED())); a.collect(AS_OF)
    b = FredAlfredCollector(_ctx(tmp_path / "b", FakeALFRED())); b.collect(AS_OF)
    ids_a = sorted(r["record_id"] for r in a.records)
    ids_b = sorted(r["record_id"] for r in b.records)
    assert ids_a == ids_b and ids_a
    # source_native_id encodes (series, observation, vintage)
    for r in a.records:
        pl = r["normalized_payload"]
        assert r["source_native_id"] == "%s|%s|%s" % (
            pl["series_id"], pl["observation_date"], pl["realtime_start"])


def test_default_path_unchanged_without_historical_config(tmp_path):
    t = FakeALFRED()
    coll = FredAlfredCollector(_ctx(tmp_path, t, historical=False))
    coll.collect(AS_OF)
    # default rolling-window path: exactly one realtime window per series, and it
    # equals observation_start = as_of - observation_window_days (NOT chunked)
    for sid in ("TESTDLY", "TESTCAP"):
        starts = {urllib.parse.parse_qs(urllib.parse.urlsplit(u).query)["realtime_start"][0]
                  for u in t.calls if "series_id=%s" % sid in u}
        assert len(starts) == 1
        assert starts == {"2021-04-17"}  # 2021-06-01 minus 45 days
