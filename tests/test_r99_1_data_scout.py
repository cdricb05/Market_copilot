"""R99.1 - the persistent data scout (alpha_agent.r59.data_scout).

Hermetic: a temporary research memory, fixture archives, a fixture R98 matrix
and an injected fetcher. Nothing here opens the live research store or the
network. The registry is always re-read from a FRESH handle on the same file,
so every assertion is about saved state, not an in-process object.
"""
from __future__ import annotations

import gzip
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent import r59  # noqa: E402
from paper_trader.alpha_agent.r59 import data_scout as DS  # noqa: E402
from paper_trader.alpha_agent.r59 import memory as M  # noqa: E402
from paper_trader.alpha_agent.r59 import opportunities as OPP  # noqa: E402

NOW = datetime(2026, 10, 4, 22, 0, tzinfo=timezone.utc)
UNIVERSE = ["AAA.US", "BBB.US", "CCC.US"]


def _mem(tmp_path) -> M.ResearchMemory:
    return M.open_memory(tmp_path / "research_memory.sqlite")


def _fixture_roots(tmp_path, day="2026-10-02", n_news=3, n_vint=3):
    ing = tmp_path / "ingestion"
    v = ing / "vintages"
    news = v / "eodhd_forward_archive" / "news"
    news.mkdir(parents=True)
    conn = sqlite3.connect(news / "news_archive.sqlite")
    conn.execute("CREATE TABLE symbol_days (query_symbol TEXT, capture_date TEXT)")
    conn.executemany("INSERT INTO symbol_days VALUES (?,?)",
                     [(s, day) for s in UNIVERSE[:n_news]])
    conn.commit()
    conn.close()
    snaps = v / "eodhd_forward_archive" / "economic_events" / "snapshots"
    snaps.mkdir(parents=True)
    (snaps / ("%s.json.gz" % day)).write_bytes(gzip.compress(b"[]"))
    adir = v / "eodhd_analyst" / day
    adir.mkdir(parents=True)
    for s in UNIVERSE[:n_vint]:
        (adir / ("%s.json" % s.split(".")[0])).write_text(json.dumps(
            {"earnings_history_recent": [{"epsActual": 1.0}], "shares_stats": None}))
    (v / "public_forward_archive" / "ibkr_shortable" / day).mkdir(parents=True)
    (v / "public_forward_archive" / "spdr_nav_history" / "vintages" / day).mkdir(parents=True)
    col = tmp_path / "collection"
    col.mkdir()
    (col / "source_runtime_health.json").write_text(json.dumps(
        {"sources": {"norgate_local": {"last_success_at": "%sT03:00:00+00:00" % day}}}))
    uni = tmp_path / "universe.json"
    uni.write_text(json.dumps({"symbols": UNIVERSE}))
    r98 = tmp_path / "r98.json"
    r98.write_text(json.dumps({"rows": [
        {"family": "P1", "provider": "Vendor A", "product": "Estimates PIT",
         "owned_entitlement": False, "entitlement_state": "NOT_OWNED",
         "price_if_public": "QUOTE_REQUIRED", "last_verified": "2026-10-03"},
        {"family": "P4", "provider": "FINRA", "product": "short interest",
         "owned_entitlement": False, "entitlement_state": "ALREADY_SETTLED"}]}))
    return {"ingestion_root": ing, "collection_root": col, "universe_file": uni,
            "r98_path": r98, "sample_root": tmp_path / "samples"}


def _run(mem, roots, *, mode="daily", now=NOW, **kw):
    return DS.run(mem, mode=mode, now=now, **roots, **kw)


def _rows(tmp_path) -> dict:
    fresh = M.ResearchMemory(tmp_path / "research_memory.sqlite", read_only=True)
    return {o["opportunity_id"]: o for o in fresh.opportunities()}


# --------------------------------------------------------------------------- #
def test_registry_is_saved_with_every_field_and_reloads(tmp_path):
    roots = _fixture_roots(tmp_path)
    out = _run(_mem(tmp_path), roots)
    assert out["result"] == DS.R_DELTA
    rows = _rows(tmp_path)
    scout = {k: v for k, v in rows.items() if "scout" in (v["detail"] or {})}
    assert len(scout) == len(DS.curated_catalogue()) + 2
    for oid, row in scout.items():
        block = row["detail"]["scout"]
        assert set(DS.REGISTRY_FIELDS) <= set(block), oid
        assert block["scout_state"] in DS.SCOUT_STATES
        assert row["state"] == DS.SCOUT_TO_FRONTIER[block["scout_state"]]
    # The four live vendor items, none claimed as sent.
    waiting = {k for k, v in scout.items()
               if v["detail"]["scout"]["scout_state"] == DS.S_AWAITING_VENDOR}
    assert waiting == {"DS_HOD_LEVEL2_24Y", "DS_ZACKS_NDL_EEH",
                       "DS_BLOOMBERG_PIT_ECON", "DS_INTRINIO_ZACKS"}
    for oid in waiting:
        b = scout[oid]["detail"]["scout"]
        assert b["vendor_request_sent_date"] is None
        assert b["vendor_request_status"] != "REQUEST_SENT"
    assert scout["DS_INTRINIO_ZACKS"]["detail"]["scout"]["vendor_request_status"] \
        == "EXTERNAL_PENDING_NO_RESPONSE"
    # Imported R98 rows keep their measured meaning.
    assert scout["DS_VENDOR_A_ESTIMATES_PIT"]["detail"]["scout"]["scout_state"] \
        == DS.S_QUOTE_REQUIRED
    assert scout["DS_FINRA_SHORT_INTEREST"]["detail"]["scout"]["scout_state"] \
        == DS.S_TESTED_NO_ALPHA
    # Lanes measured from the fixture archive.
    lanes = out["lanes"]
    assert lanes["EODHD_NEWS"]["status"] == DS.LANE_ACTIVE
    assert lanes["EODHD_ECON_EVENTS"]["status"] == DS.LANE_ACTIVE
    assert lanes["EODHD_EARNINGS"]["status"] == DS.LANE_ACTIVE
    assert lanes["EODHD_SHARES_STATS"]["status"] == DS.LANE_MISSING


def test_real_r98_matrix_imports_every_unabsorbed_row_once():
    rows = DS.r98_catalogue()
    ids = [r["opportunity_id"] for r in rows]
    assert len(rows) == 54 - len(DS._R98_ABSORBED) == 40
    assert len(set(ids)) == len(ids)
    curated = {e["opportunity_id"] for e in DS.curated_catalogue()}
    assert not curated & set(ids)
    assert set(DS._R98_ABSORBED.values()) <= curated


def test_rerun_same_state_is_no_material_delta_and_writes_no_row(tmp_path):
    roots = _fixture_roots(tmp_path)
    mem = _mem(tmp_path)
    _run(mem, roots)
    before = _rows(tmp_path)
    n_events = len(mem.events(limit=100000))
    assert n_events > 0
    out = _run(_mem(tmp_path), roots, now=NOW + timedelta(hours=21))
    assert out["result"] == DS.R_NO_DELTA and out["deltas"] == []
    after = _rows(tmp_path)
    assert set(after) == set(before)
    assert {k: v["updated_at"] for k, v in after.items()} == \
        {k: v["updated_at"] for k, v in before.items()}
    assert len(mem.events(limit=100000)) == n_events     # no event either


def test_auto_mode_is_not_due_inside_the_day(tmp_path):
    roots = _fixture_roots(tmp_path)
    _run(_mem(tmp_path), roots, mode="weekly")
    out = DS.run(_mem(tmp_path), mode="auto", now=NOW + timedelta(hours=3))
    assert out["result"] == DS.R_NOT_DUE
    out = DS.run(_mem(tmp_path), mode="auto", now=NOW + timedelta(hours=21))
    assert out["mode"] == "daily"            # roots were remembered from the first run
    assert out["roots_known"] is True


def test_a_stale_lane_is_a_material_delta(tmp_path):
    roots = _fixture_roots(tmp_path)
    _run(_mem(tmp_path), roots)
    out = _run(_mem(tmp_path), roots, now=NOW + timedelta(days=6))
    assert out["result"] == DS.R_DELTA
    news = _rows(tmp_path)["EODHD_NEWS_UNIVERSE"]
    assert news["detail"]["scout"]["lane_status"] == DS.LANE_STALE
    assert news["detail"]["scout"]["scout_state"] == DS.S_OWNED_UNDERUSED
    assert news["state"] == r59.DO_ALREADY_OWNED_UNUSED


def test_low_coverage_lane_is_stale(tmp_path):
    roots = _fixture_roots(tmp_path, n_news=1)
    out = _run(_mem(tmp_path), roots)
    assert out["lanes"]["EODHD_NEWS"]["status"] == DS.LANE_STALE


def test_seed_never_overwrites_a_scout_owned_row(tmp_path):
    roots = _fixture_roots(tmp_path)
    mem = _mem(tmp_path)
    _run(mem, roots)
    OPP._seed_set(mem, "EODHD_NEWS_UNIVERSE", title="stale seed claim",
                  state=r59.DO_ALREADY_OWNED_UNUSED)
    row = _rows(tmp_path)["EODHD_NEWS_UNIVERSE"]
    assert row["title"] != "stale seed claim"
    assert row["detail"]["scout"]["scout_state"] == DS.S_OWNED_ACTIVE
    OPP._seed_set(mem, "BRAND_NEW_SEED_ROW", title="t", state=r59.DO_BLOCKED)
    assert "BRAND_NEW_SEED_ROW" in _rows(tmp_path)


def _csv(path: Path, rows: list) -> None:
    cols = list(rows[0])
    path.write_text("\n".join([",".join(cols)] + [",".join(str(r[c]) for c in cols)
                                                  for r in rows]) + "\n")


def test_sample_without_declared_asof_fails_pit_and_is_never_reprocessed(tmp_path):
    roots = _fixture_roots(tmp_path)
    _run(_mem(tmp_path), roots)
    inbox = roots["sample_root"] / "DS_ZACKS_NDL_EEH" / "inbox"
    inbox.mkdir(parents=True)
    _csv(inbox / "eeh.csv", [{"ticker": "AAA", "obs_date": "2018-01-02", "eps": 1}])
    out = _run(_mem(tmp_path), roots, now=NOW + timedelta(hours=21))
    assert out["result"] == DS.R_DELTA
    b = _rows(tmp_path)["DS_ZACKS_NDL_EEH"]["detail"]["scout"]
    assert b["scout_state"] == DS.S_SAMPLE_PIT_FAILED
    assert "pit_asof_declared" in b["sample_certifications"][-1]["failed"]
    out = _run(_mem(tmp_path), roots, now=NOW + timedelta(hours=42))
    assert out["result"] == DS.R_NO_DELTA
    assert len(_rows(tmp_path)["DS_ZACKS_NDL_EEH"]["detail"]["scout"]
               ["sample_certifications"]) == 1


def test_certified_sample_opens_a_test_obligation(tmp_path):
    roots = _fixture_roots(tmp_path)
    mem = _mem(tmp_path)
    _run(mem, roots)
    row = _rows(tmp_path)["DS_ZACKS_NDL_EEH"]
    block = dict(row["detail"]["scout"])
    # The data-foundation agent declares the PIT contract of the sample.
    block["sample_spec"] = {"format": "csv", "required_columns": ["ticker", "obs_date",
                                                                  "per_end", "eps"],
                            "asof_column": "obs_date", "entity_column": "ticker",
                            "requires_vintages": True,
                            "vintage_key_columns": ["ticker", "per_end"],
                            "delisted_probe_entities": ["DEAD"], "min_entities": 2,
                            "min_history_years": 0.0}
    DS._write(mem, "DS_ZACKS_NDL_EEH", title=row["title"],
              asset_class=row["asset_class"], block=block, prior=row)
    inbox = roots["sample_root"] / "DS_ZACKS_NDL_EEH" / "inbox"
    inbox.mkdir(parents=True)
    _csv(inbox / "eeh.csv", [
        {"ticker": "AAA", "obs_date": "2018-01-02", "per_end": "2018-03-31", "eps": 1.0},
        {"ticker": "AAA", "obs_date": "2018-02-02", "per_end": "2018-03-31", "eps": 1.1},
        {"ticker": "DEAD", "obs_date": "2018-01-05", "per_end": "2018-03-31", "eps": 0.2}])
    _run(_mem(tmp_path), roots, now=NOW + timedelta(hours=21))
    b = _rows(tmp_path)["DS_ZACKS_NDL_EEH"]["detail"]["scout"]
    assert b["scout_state"] == DS.S_SAMPLE_READY
    assert b["test_obligation"]["status"] == "OPEN"
    assert b["test_obligation"]["next_steps"][-1].startswith("DISCOVERY TEST")
    assert b["test_obligation"]["novelty"]["verdict"] == "NO_MECHANISM_DECLARED"
    st = DS.status(M.ResearchMemory(tmp_path / "research_memory.sqlite", read_only=True))
    assert st["samples_ready_to_test"] == ["DS_ZACKS_NDL_EEH"]
    assert _rows(tmp_path)["DS_ZACKS_NDL_EEH"]["state"] == r59.DO_SAMPLE_UNDER_EVALUATION


def test_future_asof_fails_pit(tmp_path):
    p = tmp_path / "s.csv"
    _csv(p, [{"t": "A", "asof": "2030-01-01"}])
    cert = DS.certify_sample(p, {"asof_column": "asof"}, received_at=NOW)
    assert cert["verdict"] == DS.S_SAMPLE_PIT_FAILED
    assert "pit_no_future_asof" in cert["failed"]


def test_vendor_events_need_evidence_respect_no_contact_and_are_idempotent(tmp_path):
    roots = _fixture_roots(tmp_path)
    mem = _mem(tmp_path)
    _run(mem, roots)
    with pytest.raises(ValueError, match="EVIDENCE_REQUIRED"):
        DS.record_vendor_event(mem, "DS_HOD_LEVEL2_24Y", event="REQUEST_SENT",
                               on="2026-10-05", evidence="")
    with pytest.raises(ValueError, match="CONTACT_NOT_ALLOWED"):
        DS.record_vendor_event(mem, "DS_INTRINIO_ZACKS", event="REQUEST_SENT",
                               on="2026-10-05", evidence="email id 1")
    r1 = DS.record_vendor_event(mem, "DS_HOD_LEVEL2_24Y", event="REQUEST_SENT",
                                on="2026-10-05", evidence="support form ticket 77")
    r2 = DS.record_vendor_event(mem, "DS_HOD_LEVEL2_24Y", event="REQUEST_SENT",
                                on="2026-10-05", evidence="support form ticket 77")
    assert r1["recorded"] and r2["duplicate"]
    b = _rows(tmp_path)["DS_HOD_LEVEL2_24Y"]["detail"]["scout"]
    assert b["vendor_request_status"] == "REQUEST_SENT"
    assert b["vendor_request_sent_date"] == "2026-10-05"
    assert b["vendor_followup_due"] == "2026-10-15"
    assert len(b["vendor_log"]) == 1
    out = _run(_mem(tmp_path), roots, now=datetime(2026, 10, 16, 12, tzinfo=timezone.utc))
    hod = [d for d in out["deltas"] if d["opportunity_id"] == "DS_HOD_LEVEL2_24Y"]
    assert hod and "VENDOR_FOLLOW_UP_DUE" in hod[0]["after"]["human_action_required"]


def test_weekly_auto_download_only_for_declared_free_samples(tmp_path):
    roots = _fixture_roots(tmp_path)
    mem = _mem(tmp_path)
    calls = []

    def fetch(url):
        calls.append(url)
        return b"t,asof\nA,2020-01-01\n"
    _run(mem, roots, mode="weekly", fetcher=fetch)
    assert calls == []                                   # nothing is declared
    row = _rows(tmp_path)["DS_CBOE_PUTCALL_DAILY"]
    block = dict(row["detail"]["scout"])
    block["free_sample"] = {"url": "https://example.org/s.csv", "filename": "s.csv",
                            "auto_download_allowed": True}
    DS._write(mem, "DS_CBOE_PUTCALL_DAILY", title=row["title"],
              asset_class=row["asset_class"], block=block, prior=row)
    _run(_mem(tmp_path), roots, mode="weekly", now=NOW + timedelta(days=7), fetcher=fetch)
    assert calls == ["https://example.org/s.csv"]
    assert list((roots["sample_root"] / "DS_CBOE_PUTCALL_DAILY" / "inbox").iterdir())
    _run(_mem(tmp_path), roots, mode="daily", now=NOW + timedelta(days=8), fetcher=fetch)
    assert calls == ["https://example.org/s.csv"]        # daily never downloads


def test_cli_door_status_is_read_only_and_vendor_event_refuses_without_evidence(tmp_path, capsys):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import alpha_agents_v2 as CLI  # noqa: E402
    roots = _fixture_roots(tmp_path)
    _run(_mem(tmp_path), roots)
    db = str(tmp_path / "research_memory.sqlite")
    assert CLI.main(["data-scout-status", "--memory", db]) == 0
    assert "PIPELINE_OK data-scout-status" in capsys.readouterr().out
    ev = tmp_path / "ev.json"
    ev.write_text(json.dumps({"opportunity_id": "DS_HOD_LEVEL2_24Y",
                              "event": "REQUEST_SENT", "on": "2026-10-05"}))
    assert CLI.main(["data-scout", "--mode", "vendor-event", "--input", str(ev),
                     "--memory", db]) == 3
    assert "PIPELINE_REFUSED EVIDENCE_REQUIRED" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# The EODHD vintage now keeps the snapshot-only SharesStats and Holders.
# --------------------------------------------------------------------------- #
def test_analyst_vintage_captures_shares_stats_and_holders(tmp_path):
    from urllib.parse import parse_qs, urlparse
    from paper_trader.alpha_agent.collectors import CollectorContext, RawArchive
    from paper_trader.alpha_agent.collectors.eodhd_analyst import EodhdAnalystCollector

    calls = []

    def transport(req, timeout):
        calls.append(req["url"])
        body = {"General": {"Name": "X", "CIK": "1"}, "Highlights": {},
                "AnalystRatings": {"Rating": 4.0},
                "Earnings": {"Trend": {}, "History": {}},
                "SharesStats": {"SharesFloat": 100.0, "ShortPercentFloat": 0.03,
                                "Unlisted": 1},
                "Holders": {"Institutions": {str(i): {"name": "I%d" % i,
                                                      "totalShares": float(i)}
                                             for i in range(15)},
                            "Funds": {"0": {"name": "F0", "totalShares": 2.0}}}}
        return {"status": 200, "headers": {}, "body": json.dumps(body).encode(),
                "error": None}

    def ctx(cfg):
        c = {"limits": {"max_retries": 1, "backoff_base_seconds": 0.0},
             "secret_redaction": {"redacted_query_params": ["api_token"],
                                  "redaction_placeholder": "REDACTED"}}
        return CollectorContext(
            config=c, source_cfg=cfg,
            archive=RawArchive(tmp_path / "raw", tmp_path, set(), 10_000_000),
            transport=transport, now_iso=lambda: "2026-10-05T15:00:00+00:00",
            clock=lambda: 0.0, sleep=lambda s: None, secrets=["K"],
            env={"EODHD_API_KEY": "K"})

    cfg = {"enabled": True, "allowed_env_vars": ["EODHD_API_KEY"],
           "sample_symbols": ["S1.US"], "vintage_subdir": "vintages/eodhd_analyst",
           "analyst_filter": "General,Highlights,AnalystRatings,Earnings"}
    EodhdAnalystCollector(ctx(cfg)).collect("2026-10-05")
    filt = parse_qs(urlparse(calls[-1]).query)["filter"][0]
    assert filt == "General,Highlights,AnalystRatings,Earnings,SharesStats,Holders"
    v = json.loads((tmp_path / "vintages/eodhd_analyst/2026-10-05/S1.json").read_text())
    assert v["shares_stats"] == {"SharesFloat": 100.0, "ShortPercentFloat": 0.03}
    assert [h["name"] for h in v["holders_top"]["Institutions"]][:2] == ["I14", "I13"]
    assert len(v["holders_top"]["Institutions"]) == 10
    calls.clear()
    EodhdAnalystCollector(ctx(dict(cfg, capture_shares_holders=False,
                                   vintage_subdir="vintages/off"))).collect("2026-10-05")
    assert parse_qs(urlparse(calls[-1]).query)["filter"][0] == \
        "General,Highlights,AnalystRatings,Earnings"
