"""R96.1 H33 — Japan MoF weekly securities + SNB weekly sight deposits, archived as
official first-release snapshots inside the canonical public_forward_archive source.
Hermetic: fake transport, tmp_path."""
from __future__ import annotations

import gzip
import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent.collectors import (  # noqa: E402
    COLLECTOR_CLASSES, CollectorContext, RawArchive)
from paper_trader.alpha_agent.collectors import forward_archive as FA  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
DAY1, DAY2 = "2026-10-02", "2026-10-05"


def _snb(publishing: str, rows: list) -> bytes:
    lines = ['\ufeff"CubeId";"snbgwdchfsgw"', '"PublishingDate";"%s"' % publishing, "",
             '"Date";"D0";"Value"']
    lines += ['"%s";"%s";%s' % (d, s, ('"%s"' % v) if v is not None else "") for d, s, v in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _mof(update: str, rows: list) -> bytes:
    hdr = [["対外及び対内証券売買契約等の状況", "", "", ""],
           ["International Transactions in Securities (Weekly)", "", "", '"Final Update  %s"' % update],
           ["", "", "", ""], ["", "1. Portfolio Investment Assets", "", ""], ["", "", "", ""],
           ["期間\nPeriod", "株式", "", ""],
           ["", "Equity and investment fund shares", "", ""],
           ["", "取得", "処分", "ネット"], ["", "Acquisition", "Disposition", "Net"]]
    out = []
    for r in hdr:
        out.append(",".join('"%s"' % c if ("\n" in c or "," in c) else c for c in r))
    for period, a, b, n in rows:
        out.append('%s,"%s ","%s ","%s "' % (period, a, b, n))
    out.append(",,,")
    out.append("   (Note 3),net acquisition with a plus sign,,")
    return ("\r\n".join(out) + "\r\n").encode("cp932")


class Transport:
    def __init__(self, bodies: dict, status: dict = None):
        self.bodies, self.status, self.calls = bodies, status or {}, []

    def __call__(self, req, timeout):
        url = req["url"]
        self.calls.append(url)
        for key, body in self.bodies.items():
            if key in url:
                return {"status": self.status.get(key, 200), "headers": {}, "body": body, "error": None}
        return {"status": 404, "headers": {}, "body": b"", "error": None}


def _ctx(tmp_path, transport, now="2026-10-02T15:00:00+00:00"):
    cfg = {"limits": {"max_retries": 0, "backoff_base_seconds": 0.0}}
    archive = RawArchive(tmp_path / "raw", tmp_path, set(), 10_000_000)
    src = {"families": [FA.FAMILY_MOF, FA.FAMILY_SNB], "min_interval_seconds": 0.0}
    return CollectorContext(config=cfg, source_cfg=src, archive=archive, transport=transport,
                            now_iso=lambda: now, clock=lambda: 0.0, sleep=lambda s: None,
                            secrets=[], env={})


def _bodies(day=1):
    snb = _snb("2026-09-28 10:00", [("2026-09-18", "GI", "430000"), ("2026-09-25", "GI", "432037"),
                                    ("2026-09-25", "TG", "456833"), ("2026-09-25", "UEB", None)])
    mof = _mof("October  1 , 2026", [("2026．9．13～9．19", "33,405", "32,963", "443"),
                                     ("2026．9．20～9．26", "18,896", "16,637", "2,258")])
    if day == 2:
        snb = _snb("2026-10-05 10:00", [("2026-09-18", "GI", "430000"), ("2026-09-25", "GI", "432100"),
                                        ("2026-09-25", "TG", "456833"), ("2026-10-02", "GI", "440000")])
        mof = _mof("October  8 , 2026", [("2026．9．13～9．19", "33,405", "32,963", "443"),
                                         ("2026．9．20～9．26", "18,896", "16,637", "2,258"),
                                         ("2026．9．27～10．3", "20,000", "21,000", "-1,000")])
    return {"snbgwdchfsgw": snb, "week.csv": mof}


def _root(tmp_path):
    return tmp_path / "vintages" / "public_forward_archive"


def _db(tmp_path, sub):
    return sqlite3.connect(str(_root(tmp_path) / sub / "first_release.sqlite"))


def test_h33_families_live_in_the_canonical_source_and_config():
    assert COLLECTOR_CLASSES["public_forward_archive"] is FA.PublicForwardArchiveCollector
    cfg = json.loads((REPO / "configs" / "alpha_agent" / "stage2_ingestion.json").read_text(encoding="utf-8"))
    fams = cfg["sources"]["public_forward_archive"]["families"]
    assert FA.FAMILY_MOF in fams and FA.FAMILY_SNB in fams
    assert "mof.go.jp" in cfg["sources"]["public_forward_archive"]["mof_weekly_url"]
    assert "data.snb.ch" in cfg["sources"]["public_forward_archive"]["snb_sight_deposits_url"]


def test_first_capture_is_a_baseline_never_a_first_release(tmp_path):
    r = FA.PublicForwardArchiveCollector(_ctx(tmp_path, Transport(_bodies()))).collect(DAY1)
    inv = r["inventory"]["forward_archive"]
    for fam, sub in ((FA.FAMILY_MOF, "japan_mof_weekly_securities"),
                     (FA.FAMILY_SNB, "snb_weekly_sight_deposits")):
        s = inv[fam]
        assert s["state"] == "HEALTHY" and s["snapshot_written"] is True
        assert s["ledger"] == "BASELINE_AT_ARCHIVE_START" and s["cells_first_observed"] > 0
        con = _db(tmp_path, sub)
        classes = {c for (c,) in con.execute("select distinct capture_class from observation")}
        assert classes == {"BASELINE_AT_ARCHIVE_START"}
        con.close()
    snb = inv[FA.FAMILY_SNB]
    assert snb["provider_release_id"] == "snbgwdchfsgw@2026-09-28 10:00"
    assert snb["latest_period"] == "2026-09-25"
    assert inv[FA.FAMILY_MOF]["latest_period"] == "2026．9．20～9．26"


def test_snapshot_preserves_raw_bytes_hash_and_both_timestamps(tmp_path):
    bodies = _bodies()
    FA.PublicForwardArchiveCollector(_ctx(tmp_path, Transport(bodies))).collect(DAY1)
    d = _root(tmp_path) / "snb_weekly_sight_deposits" / "snapshots" / DAY1
    raw = gzip.decompress((d / "snbgwdchfsgw.csv.gz").read_bytes())
    assert raw == bodies["snbgwdchfsgw"]
    meta = json.loads((d / "snbgwdchfsgw.csv.meta.json").read_text(encoding="utf-8"))
    import hashlib
    assert meta["source_hash"] == hashlib.sha256(raw).hexdigest()
    assert meta["observed_at"] == "2026-10-02T15:00:00+00:00"            # our capture instant
    assert meta["provider_release_timestamp"] == "2026-09-28 10:00 Europe/Zurich"
    mof = gzip.decompress((_root(tmp_path) / "japan_mof_weekly_securities" / "snapshots" / DAY1
                           / "week.csv.gz").read_bytes())
    assert mof == bodies["week.csv"]


def test_same_day_rerun_is_a_no_op_without_network(tmp_path):
    t = Transport(_bodies())
    FA.PublicForwardArchiveCollector(_ctx(tmp_path, t)).collect(DAY1)
    n = len(t.calls)
    r = FA.PublicForwardArchiveCollector(_ctx(tmp_path, t)).collect(DAY1)
    assert len(t.calls) == n
    for fam in (FA.FAMILY_MOF, FA.FAMILY_SNB):
        assert r["inventory"]["forward_archive"][fam]["snapshot_already_archived"] is True
    con = _db(tmp_path, "snb_weekly_sight_deposits")
    assert con.execute("select count(*) from snapshot").fetchone()[0] == 1
    con.close()


def test_later_release_is_first_release_and_a_revision_is_appended_not_overwritten(tmp_path):
    FA.PublicForwardArchiveCollector(_ctx(tmp_path, Transport(_bodies(1)))).collect(DAY1)
    r = FA.PublicForwardArchiveCollector(
        _ctx(tmp_path, Transport(_bodies(2)), now="2026-10-05T15:00:00+00:00")).collect(DAY2)
    s = r["inventory"]["forward_archive"][FA.FAMILY_SNB]
    assert s["ledger"] == "FIRST_RELEASE_OBSERVED" and s["cells_first_observed"] == 1
    assert s["cells_revised"] == 1
    con = _db(tmp_path, "snb_weekly_sight_deposits")
    new = con.execute("select value, capture_class, first_observed_at from observation "
                      "where series='GI' and period='2026-10-02'").fetchone()
    assert new == ("440000", "FIRST_RELEASE_OBSERVED", "2026-10-05T15:00:00+00:00")
    first = con.execute("select value from observation where series='GI' and period='2026-09-25'").fetchone()
    assert first == ("432037",)                                          # first value preserved
    rev = con.execute("select old_value, new_value, snapshot_date from revision").fetchall()
    assert rev == [("432037", "432100", DAY2)]
    con.close()
    m = r["inventory"]["forward_archive"][FA.FAMILY_MOF]
    assert m["ledger"] == "FIRST_RELEASE_OBSERVED" and m["cells_first_observed"] == 3
    assert m["cells_revised"] == 0


def test_error_page_or_404_writes_no_snapshot(tmp_path):
    bodies = _bodies()
    bodies["snbgwdchfsgw"] = b"<!DOCTYPE html><html>maintenance</html>"
    t = Transport(bodies, status={"week.csv": 404})
    r = FA.PublicForwardArchiveCollector(_ctx(tmp_path, t)).collect(DAY1)
    inv = r["inventory"]["forward_archive"]
    assert inv[FA.FAMILY_SNB]["state"] == "FAILED" and inv[FA.FAMILY_MOF]["state"] == "FAILED"
    assert not (_root(tmp_path) / "snb_weekly_sight_deposits" / "snapshots" / DAY1).exists()
    assert not (_root(tmp_path) / "japan_mof_weekly_securities" / "snapshots" / DAY1).exists()


def test_mof_values_are_kept_verbatim_no_sign_or_unit_reinterpretation():
    p = FA.parse_mof_weekly(_bodies(2)["week.csv"])
    cells = {(s, per): v for s, per, v in p["rows"]}
    assert cells[("col_03", "2026．9．27～10．3")] == "-1,000"
    assert cells[("col_01", "2026．9．27～10．3")] == "20,000"
    assert "Acquisition" in p["columns"]["col_01"]
    assert p["release_id"].startswith('"Final Update') or "Final Update" in p["release_id"]


@pytest.mark.skipif(not Path(r"D:\Stock_Prediction_app_data\r91_alpha_search_liberation"
                             r"\mof_securities_flows\week_data.csv").exists(),
                    reason="R91 MoF file not on this machine")
def test_mof_parser_reads_the_real_official_layout():
    raw = Path(r"D:\Stock_Prediction_app_data\r91_alpha_search_liberation\mof_securities_flows"
               r"\week_data.csv").read_bytes()
    p = FA.parse_mof_weekly(raw)
    periods = {per for _, per, _ in p["rows"]}
    assert len(periods) == 1134                       # R91 certification: 1,134 weeks
    assert p["latest_period"] == "2026．9．20～9．26"
