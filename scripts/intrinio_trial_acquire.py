#!/usr/bin/env python
r"""Intrinio TRIAL research acquisition CLI (research-only, credential-safe).

Operator-triggered acquisition for the operator-approved Intrinio live trial.
Composes the RELEASED collector machinery (``alpha_agent.collectors.intrinio``
over ``BaseCollector.fetch``: rate limiting, bounded retries + backoff, circuit
breaker, response hygiene, secret redaction, content-addressed immutable
RawArchive) — no parallel HTTP framework. Three bounded, resumable commands:

  --zacks-snapshot        capture TODAY's genuine full-cross-section snapshot of
                          the four entitled Zacks feeds (EPS/Sales Estimates as
                          the CURRENT forward-consensus table; EPS/Sales
                          Surprises as the shallow entitled event history from
                          2024-12-01). Idempotent per (feed, as_of): an existing
                          snapshot manifest is never overwritten (use --force to
                          re-capture into a superseding manifest revision).
  --fundamentals-metadata walk the survivorship-safe Norgate universe from the
                          canonical IdentityStore (CIK-first identity; ticker
                          text alone NEVER resolves identity) and archive each
                          issuer's full US-Fundamentals metadata history
                          (fiscal periods, filing_date, first_calculable,
                          earnings_disclosed_at, reported/restated type).
  --fundamentals-tags     for CIK-verified issuers only, archive full quarterly
                          standardized history for the bounded registered tag
                          set (13 tags) via /historical_data.

Safety: TRIAL / research-only. Writes ONLY under the trial research root on D:
(raw archive + derived JSON + checkpoints). Never writes an operational ledger,
paper book, order, NAV, or target state. Never prints or persists the API key.
No cadence, no scheduled task, no purchase, no billing action.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import re
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import source_contracts as sc                # noqa: E402
from paper_trader.alpha_agent.collectors.base import (                     # noqa: E402
    CollectorContext, RawArchive, default_transport)
from paper_trader.alpha_agent.collectors.intrinio import IntrinioCollector  # noqa: E402
from paper_trader.alpha_agent.historical_identity import IdentityStore     # noqa: E402

SCHEMA_VERSION = "intrinio-trial-acquire-1.0.0"
IDENTITY_DB = Path(r"D:\Stock_Prediction_app_data\alpha_agent\identity\historical_identity.sqlite")

# Bounded registered tag set: exactly what the pre-registered signal family
# (profitability/quality, accruals, growth, fundamental momentum, fcf) and the
# restatement-bias audit consume — NOT an open-ended field sweep. (The first 120
# acquired issuers carry 6 additional exploratory tags from the initial wider
# set; those are unused by every signal and harmless.)
FUNDAMENTAL_TAGS = (
    "totalrevenue", "totalgrossprofit", "netincome", "totalassets",
    "totalequity", "netcashfromoperatingactivities",
    "purchaseofplantpropertyandequipment",
)

ZACKS_FEEDS = {
    # feed_key -> (path template, response list key, dedupe_last_per_key)
    # Estimates are captured WINDOWED by fiscal-period-end (the 'date' filter):
    # a full-table walk exceeds 750k rows (mostly far-future periods) and the
    # provider answers HTTP 401 beyond ~74 pagination pages of one walk
    # (measured live 2026-08-10). Near-term forward periods are the genuine
    # current cross-section. The raw table holds ~4 near-identical vintage rows
    # per (company, period) WITHOUT a vintage timestamp -> the normalized
    # snapshot keeps the LAST row per key (pagination is (date,id)-ordered so
    # the last row is the newest vintage) and is honestly CURRENT-only.
    "eps_estimates": ("/zacks/eps_estimates?page_size=10000"
                      "&start_date=%(est_start)s&end_date=%(est_end)s",
                      "estimates", True),
    "sales_estimates": ("/zacks/sales_estimates?page_size=10000"
                        "&start_date=%(est_start)s&end_date=%(est_end)s",
                        "estimates", True),
    "eps_surprises": ("/zacks/eps_surprises?page_size=10000&start_date=2024-12-01&end_date=%(as_of)s",
                      "eps_surprises", False),
    "sales_surprises": ("/zacks/sales_surprises?page_size=10000&start_date=2024-12-01&end_date=%(as_of)s",
                        "sales_surprises", False),
}


def _estimate_window(as_of: str) -> dict:
    """Near-term forward fiscal-period-end window: from the current quarter's
    start to ~10 forward quarters (the genuine current consensus cross-section)."""
    d = _dt.date.fromisoformat(as_of)
    start = _dt.date(d.year, 3 * ((d.month - 1) // 3) + 1, 1) - _dt.timedelta(days=95)
    end = _dt.date(d.year + 2, 12, 31)
    return {"est_start": start.isoformat(), "est_end": end.isoformat(), "as_of": as_of}

# Norgate delisted symbols carry a -YYYYMM suffix (e.g. WFM-201708).
_DELISTED_SUFFIX = re.compile(r"^(?P<base>.+)-(?P<yyyymm>\d{6})$")

RS_MATCHED_CIK = "MATCHED_CIK"
RS_TICKER_ONLY_UNVERIFIED = "TICKER_ONLY_UNVERIFIED"
RS_CIK_MISMATCH = "CIK_MISMATCH"
RS_NOT_FOUND = "NOT_FOUND"
RS_ERROR = "ERROR"


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _load_cfg() -> dict:
    p = _REPO / "configs" / "alpha_agent" / "intrinio_trial.json"
    return json.loads(p.read_text(encoding="utf-8"))


def _research_root(cfg: dict) -> Path:
    return Path(cfg.get("research_root")
                or r"D:\Stock_Prediction_app_data\provider_trials\intrinio")


def _known_raw_ids(research_root: Path) -> set:
    """Preload the content-addressed raw ids so re-runs never duplicate."""
    raw = research_root / "raw"
    if not raw.is_dir():
        return set()
    return {p.stem for p in raw.rglob("*.metadata.json")}


def build_collector(cfg: dict, *, rate: float | None = None) -> IntrinioCollector:
    import os
    run_cfg = dict(cfg.get("run") or {})
    source_cfg = dict(cfg.get("source") or {})
    if rate is not None:
        source_cfg["min_interval_seconds"] = float(rate)
    root = _research_root(cfg)
    root.mkdir(parents=True, exist_ok=True)
    max_bytes = int((run_cfg.get("limits") or {}).get("raw_object_max_bytes", 33554432))
    archive = RawArchive(raw_root=root / "raw", output_root=root,
                         known_ids=_known_raw_ids(root), max_bytes=max_bytes)
    ctx = CollectorContext(
        config=run_cfg, source_cfg=source_cfg, archive=archive,
        transport=default_transport, now_iso=_now_iso, clock=time.monotonic,
        sleep=time.sleep, secrets=[], user_agent=run_cfg.get("user_agent"),
        env=dict(os.environ), identity=sc.IdentityResolver())
    return IntrinioCollector(ctx)


def _parse_json(body):
    if not body:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None


def base_ticker(norgate_symbol: str) -> str:
    """Strip the Norgate delisted -YYYYMM suffix; NEVER used as identity proof,
    only as a lookup attempt that must be CIK-verified afterwards."""
    m = _DELISTED_SUFFIX.match(norgate_symbol or "")
    return m.group("base") if m else (norgate_symbol or "")


def _norm_cik(v) -> str:
    s = re.sub(r"\D", "", str(v or ""))
    return s.zfill(10) if s else ""


def _safe_name(security_id: str) -> str:
    """Windows-safe filename for a security_id (e.g. 'ngid:124589')."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", security_id or "")


def _atomic_write(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _load_checkpoint(path: Path) -> dict:
    if path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"done": {}, "updated_at": None}


# --------------------------------------------------------------------------- #
# Zacks full-cross-section snapshot
# --------------------------------------------------------------------------- #
def run_zacks_snapshot(c: IntrinioCollector, key: str, root: Path, as_of: str,
                       *, force: bool = False, max_pages: int = 80) -> dict:
    out_summary = {}
    window = _estimate_window(as_of)
    for feed, (tmpl, list_key, dedupe_last) in ZACKS_FEEDS.items():
        snap_dir = root / "zacks_snapshots" / feed
        manifest_path = snap_dir / ("%s.manifest.json" % as_of)
        if manifest_path.is_file() and not force:
            out_summary[feed] = {"status": "ALREADY_CAPTURED", "manifest": str(manifest_path)}
            print("  %-16s ALREADY_CAPTURED (idempotent; --force to supersede)" % feed)
            continue
        path = tmpl % window
        rows, raw_ids, pages = [], [], 0
        next_page = None
        while pages < max_pages:
            url = c._url(path if next_page is None else "%s&next_page=%s" % (path, next_page))
            res = c.fetch(url, headers=c._headers(key), expect="text",
                          extension="json", business_date=as_of,
                          native_id="intrinio|zacks_snapshot|%s|%s|p%03d" % (feed, as_of, pages),
                          content_type="application/json",
                          license_note=c.ctx.source_cfg.get("license_note"))
            if not res["ok"]:
                out_summary[feed] = {"status": "FETCH_FAILED",
                                     "http_status": res.get("status"),
                                     "rejected_reason": res.get("rejected_reason"),
                                     "error": res.get("error"), "pages_ok": pages}
                break
            obj = _parse_json(res["body"])
            if not isinstance(obj, dict):
                out_summary[feed] = {"status": "PARSE_FAILED", "pages_ok": pages}
                break
            page_rows = obj.get(list_key) if isinstance(obj.get(list_key), list) else []
            rows.extend(page_rows)
            if res.get("raw"):
                raw_ids.append(res["raw"]["raw_object_id"])
            pages += 1
            next_page = obj.get("next_page")
            print("  %-16s page %3d rows=%6d total=%7d next=%s"
                  % (feed, pages, len(page_rows), len(rows), bool(next_page)))
            if not next_page:
                break
        else:
            out_summary[feed] = {"status": "PAGE_CAP_REACHED", "pages": pages,
                                 "cursor": next_page, "rows": len(rows)}
        if feed in out_summary and out_summary[feed].get("status") in (
                "FETCH_FAILED", "PARSE_FAILED", "PAGE_CAP_REACHED"):
            continue
        retrieved = _now_iso()
        n_raw_rows = len(rows)
        if dedupe_last:
            # keep the LAST (newest-vintage) row per (company, period-end,
            # fiscal_year, fiscal_period); the walk is (date,id)-ordered.
            last = {}
            for r in rows:
                comp = r.get("company") or {}
                k = (comp.get("id"), r.get("date"), r.get("fiscal_year"),
                     str(r.get("fiscal_period")))
                last[k] = r
            rows = list(last.values())
        jsonl = snap_dir / ("%s.jsonl" % as_of)
        snap_dir.mkdir(parents=True, exist_ok=True)
        with jsonl.open("w", encoding="utf-8") as f:
            for r in rows:
                rec = {"schema_version": SCHEMA_VERSION, "provider": "Intrinio",
                       "feed": feed, "snapshot_as_of": as_of,
                       "retrieved_at": retrieved, "license_state": "TRIAL",
                       "research_use_only": True,
                       "observation_kind": ("CURRENT_CONSENSUS_SNAPSHOT"
                                            if "estimates" in feed else
                                            "SHALLOW_EVENT_HISTORY"),
                       "payload": r}
                f.write(json.dumps(rec, sort_keys=True) + "\n")
        manifest = {"schema_version": SCHEMA_VERSION, "provider": "Intrinio",
                    "feed": feed, "snapshot_as_of": as_of, "retrieved_at": retrieved,
                    "rows": len(rows), "raw_rows_before_vintage_dedupe": n_raw_rows,
                    "estimate_window": (window if dedupe_last else None),
                    "pages": pages, "raw_object_ids": raw_ids,
                    "jsonl": jsonl.name, "license_state": "TRIAL",
                    "research_use_only": True, "operational_use_allowed": False,
                    "pit_note": ("estimates feeds are CURRENT consensus snapshots "
                                 "(availability = retrieval time); surprise feeds "
                                 "are the shallow entitled event history "
                                 "(~2025-01 onward) and are NOT historical "
                                 "point-in-time revision data")}
        _atomic_write(manifest_path, manifest)
        out_summary[feed] = {"status": "CAPTURED", "rows": len(rows), "pages": pages}
    return out_summary


# --------------------------------------------------------------------------- #
# Zacks estimates: per-universe capture (the reliable estimates path).
# The GLOBAL estimates stream is vintage-inflated (~25 near-identical vintage
# rows per company-period near-term, no vintage timestamp) and rate-hostile
# (measured live: HTTP 401 beyond ~74 pages of one walk, HTTP 429 on repeated
# 10k-row pages). Per-identifier requests return one company's full estimate
# rows in ONE call — the correct bounded shape for the operating universe and
# for idempotent 30-day forward captures.
# --------------------------------------------------------------------------- #
def run_zacks_estimates_universe(c: IntrinioCollector, key: str, root: Path,
                                 as_of: str, *, force: bool = False) -> dict:
    store = IdentityStore(IDENTITY_DB)
    members = [u for u in _universe(store) if u["is_current"]]
    out = {}
    for feed, path_tmpl, list_key in (
            ("eps_estimates_universe", "/zacks/eps_estimates?identifier=%s&page_size=1000", "estimates"),
            ("sales_estimates_universe", "/zacks/sales_estimates?identifier=%s&page_size=1000", "estimates")):
        snap_dir = root / "zacks_snapshots" / feed
        manifest_path = snap_dir / ("%s.manifest.json" % as_of)
        if manifest_path.is_file() and not force:
            out[feed] = {"status": "ALREADY_CAPTURED", "manifest": str(manifest_path)}
            print("  %-24s ALREADY_CAPTURED (idempotent)" % feed)
            continue
        rows, raw_ids, misses = [], [], 0
        for i, u in enumerate(members):
            ident = base_ticker(u["norgate_symbol"] or u["ticker"] or "")
            if not ident:
                continue
            res = c.fetch(c._url(path_tmpl % ident), headers=c._headers(key),
                          expect="text", extension="json", business_date=as_of,
                          allow_404=True,
                          native_id="intrinio|%s|%s|%s" % (feed, as_of, u["security_id"]),
                          content_type="application/json",
                          license_note=c.ctx.source_cfg.get("license_note"))
            if not res["ok"]:
                misses += 1
                continue
            obj = _parse_json(res["body"])
            page_rows = (obj or {}).get(list_key) or []
            # newest-vintage per (period,fy,fp); rows are append-ordered
            last = {}
            for r in page_rows:
                k = (r.get("date"), r.get("fiscal_year"), str(r.get("fiscal_period")))
                last[k] = r
            for r in last.values():
                r["_security_id"] = u["security_id"]
                r["_norgate_assetid"] = u["norgate_assetid"]
                rows.append(r)
            if res.get("raw"):
                raw_ids.append(res["raw"]["raw_object_id"])
            if (i + 1) % 100 == 0:
                print("  %-24s [%d/%d] rows=%d misses=%d"
                      % (feed, i + 1, len(members), len(rows), misses))
            if c.circuit_state != "CLOSED":
                print("  CIRCUIT_OPEN — aborting feed")
                break
        retrieved = _now_iso()
        snap_dir.mkdir(parents=True, exist_ok=True)
        jsonl = snap_dir / ("%s.jsonl" % as_of)
        with jsonl.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps({
                    "schema_version": SCHEMA_VERSION, "provider": "Intrinio",
                    "feed": feed, "snapshot_as_of": as_of,
                    "retrieved_at": retrieved, "license_state": "TRIAL",
                    "research_use_only": True,
                    "observation_kind": "CURRENT_CONSENSUS_SNAPSHOT",
                    "payload": r}, sort_keys=True) + "\n")
        _atomic_write(manifest_path, {
            "schema_version": SCHEMA_VERSION, "provider": "Intrinio", "feed": feed,
            "snapshot_as_of": as_of, "retrieved_at": retrieved, "rows": len(rows),
            "universe": "norgate_current_members", "members": len(members),
            "misses": misses, "raw_object_ids_n": len(raw_ids),
            "license_state": "TRIAL", "research_use_only": True,
            "pit_note": "CURRENT consensus snapshot per current member; availability = retrieval time"})
        out[feed] = {"status": "CAPTURED", "rows": len(rows), "members": len(members),
                     "misses": misses}
    return out


# --------------------------------------------------------------------------- #
# US Fundamentals: per-issuer metadata history (CIK-first identity)
# --------------------------------------------------------------------------- #
def _universe(store: IdentityStore) -> list:
    rows = store.list_securities(limit=100000)
    out = []
    for r in rows:
        m = store.active_mapping(r["security_id"])
        cik = _norm_cik(m.get("cik")) if (m and m.get("status") == "RESOLVED") else ""
        out.append({"security_id": r["security_id"],
                    "norgate_assetid": r.get("norgate_assetid"),
                    "norgate_symbol": r.get("norgate_symbol") or r.get("ticker"),
                    "ticker": r.get("ticker"),
                    "issuer_name": r.get("issuer_name"),
                    "is_current": bool(r.get("is_current")),
                    "delisting_date": r.get("delisting_date"),
                    "cik": cik})
    # deterministic order: CIK-resolved first (strong identity), then the rest
    out.sort(key=lambda x: (0 if x["cik"] else 1, x["security_id"]))
    return out


def run_fundamentals_metadata(c: IntrinioCollector, key: str, root: Path,
                              *, max_companies: int | None = None,
                              max_pages_per_company: int = 3) -> dict:
    store = IdentityStore(IDENTITY_DB)
    universe = _universe(store)
    ckpt_path = root / "fundamentals" / "checkpoints" / "metadata_checkpoint.json"
    ckpt = _load_checkpoint(ckpt_path)
    done = ckpt["done"]
    todo = [u for u in universe if u["security_id"] not in done]
    if max_companies:
        todo = todo[:max_companies]
    print("universe=%d done=%d todo=%d" % (len(universe), len(done), len(todo)))
    counts = {}
    for i, u in enumerate(todo):
        ident = u["cik"] if u["cik"] else base_ticker(u["norgate_symbol"] or "")
        state, company, funds, http_status = RS_ERROR, None, [], None
        if not ident:
            state = RS_NOT_FOUND
        else:
            next_page, pages = None, 0
            aborted = False
            while pages < max_pages_per_company:
                path = "/companies/%s/fundamentals?page_size=1000" % ident
                if next_page:
                    path += "&next_page=%s" % next_page
                res = c.fetch(c._url(path), headers=c._headers(key), expect="text",
                              extension="json", allow_404=True,
                              native_id="intrinio|fund_meta|%s|p%d" % (u["security_id"], pages),
                              content_type="application/json",
                              license_note=c.ctx.source_cfg.get("license_note"))
                http_status = res.get("status")
                if res.get("not_found"):
                    state = RS_NOT_FOUND
                    aborted = True
                    break
                if not res["ok"]:
                    state = RS_ERROR
                    aborted = True
                    break
                obj = _parse_json(res["body"])
                if not isinstance(obj, dict):
                    state = RS_ERROR
                    aborted = True
                    break
                company = obj.get("company") or company
                rows = obj.get("fundamentals") if isinstance(obj.get("fundamentals"), list) else []
                funds.extend(rows)
                pages += 1
                next_page = obj.get("next_page")
                if not next_page:
                    break
            if not aborted:
                got_cik = _norm_cik((company or {}).get("cik"))
                if u["cik"]:
                    state = RS_MATCHED_CIK if got_cik == u["cik"] else RS_CIK_MISMATCH
                else:
                    state = RS_TICKER_ONLY_UNVERIFIED
        rec = {"schema_version": SCHEMA_VERSION, "provider": "Intrinio",
               "dataset": "us_fundamentals", "retrieved_at": _now_iso(),
               "license_state": "TRIAL", "research_use_only": True,
               "security": u, "lookup_identifier_kind": "cik" if u["cik"] else "ticker",
               "resolution_state": state, "http_status": http_status,
               "intrinio_company": company, "n_fundamentals": len(funds),
               "fundamentals": funds}
        _atomic_write(root / "fundamentals" / "metadata" / ("%s.json" % _safe_name(u["security_id"])), rec)
        done[u["security_id"]] = state
        counts[state] = counts.get(state, 0) + 1
        if (i + 1) % 25 == 0 or i == len(todo) - 1:
            ckpt["updated_at"] = _now_iso()
            _atomic_write(ckpt_path, ckpt)
            print("  [%d/%d] %s  %s" % (i + 1, len(todo), u["security_id"],
                                        json.dumps(counts, sort_keys=True)))
        if c.circuit_state != "CLOSED":
            print("  CIRCUIT_OPEN — checkpointed; resume after cooldown")
            break
    ckpt["updated_at"] = _now_iso()
    _atomic_write(ckpt_path, ckpt)
    return counts


# --------------------------------------------------------------------------- #
# US Fundamentals: quarterly standardized tag history (CIK-verified only)
# --------------------------------------------------------------------------- #
def run_fundamentals_tags(c: IntrinioCollector, key: str, root: Path,
                          *, max_companies: int | None = None) -> dict:
    meta_dir = root / "fundamentals" / "metadata"
    ckpt_path = root / "fundamentals" / "checkpoints" / "tags_checkpoint.json"
    ckpt = _load_checkpoint(ckpt_path)
    done = ckpt["done"]
    # Only issuers with owned price-panel coverage can enter a cross-section;
    # tags for panel-less issuers would be dead weight against the trial quota.
    panel_file = root / "fundamentals" / "panel_assetids.json"
    panel_aids = None
    if panel_file.is_file():
        panel_aids = set(json.loads(panel_file.read_text(encoding="utf-8"))["assetids"])
        print("tags: filtering to %d owned-panel assetids" % len(panel_aids))
    eligible = []
    for p in sorted(meta_dir.glob("*.json")):
        rec = json.loads(p.read_text(encoding="utf-8"))
        if rec.get("resolution_state") == RS_MATCHED_CIK and rec.get("intrinio_company"):
            sid = rec["security"]["security_id"]
            aid = str(rec["security"].get("norgate_assetid") or "")
            if panel_aids is not None and aid not in panel_aids:
                continue
            if sid not in done:
                eligible.append({"security_id": sid,
                                 "com_id": rec["intrinio_company"].get("id"),
                                 "cik": rec["security"].get("cik")})
    if max_companies:
        eligible = eligible[:max_companies]
    print("tags: eligible_todo=%d done=%d tags_per_company=%d"
          % (len(eligible), len(done), len(FUNDAMENTAL_TAGS)))
    counts = {"OK": 0, "PARTIAL": 0, "FAILED": 0}
    for i, u in enumerate(eligible):
        series, failures = {}, []
        for tag in FUNDAMENTAL_TAGS:
            path = ("/companies/%s/historical_data/%s?frequency=quarterly&page_size=2000"
                    % (u["com_id"], tag))
            res = c.fetch(c._url(path), headers=c._headers(key), expect="text",
                          extension="json", allow_404=True,
                          native_id="intrinio|fund_tag|%s|%s" % (u["security_id"], tag),
                          content_type="application/json",
                          license_note=c.ctx.source_cfg.get("license_note"))
            if res.get("not_found"):
                series[tag] = []
                continue
            if not res["ok"]:
                failures.append(tag)
                continue
            obj = _parse_json(res["body"])
            rows = (obj or {}).get("historical_data") or []
            series[tag] = [{"date": r.get("date"), "value": r.get("value")}
                           for r in rows if isinstance(r, dict)]
        state = "OK" if not failures else ("PARTIAL" if series else "FAILED")
        rec = {"schema_version": SCHEMA_VERSION, "provider": "Intrinio",
               "dataset": "us_fundamentals_tags", "retrieved_at": _now_iso(),
               "license_state": "TRIAL", "research_use_only": True,
               "security_id": u["security_id"], "cik": u["cik"],
               "intrinio_company_id": u["com_id"], "frequency": "quarterly",
               "value_vintage_note": ("historical_data returns the LATEST "
                                      "standardized values (restatements folded "
                                      "in); PIT availability must come from the "
                                      "per-period fundamentals metadata "
                                      "(filing_date/first_calculable)"),
               "failed_tags": failures, "series": series}
        _atomic_write(root / "fundamentals" / "tags" / ("%s.json" % _safe_name(u["security_id"])), rec)
        done[u["security_id"]] = state
        counts[state] = counts.get(state, 0) + 1
        if (i + 1) % 10 == 0 or i == len(eligible) - 1:
            ckpt["updated_at"] = _now_iso()
            _atomic_write(ckpt_path, ckpt)
            print("  [%d/%d] %s %s" % (i + 1, len(eligible), u["security_id"],
                                       json.dumps(counts, sort_keys=True)))
        if c.circuit_state != "CLOSED":
            print("  CIRCUIT_OPEN — checkpointed; resume after cooldown")
            break
    ckpt["updated_at"] = _now_iso()
    _atomic_write(ckpt_path, ckpt)
    return counts


# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description="Intrinio TRIAL research acquisition (research-only).")
    ap.add_argument("--zacks-snapshot", action="store_true")
    ap.add_argument("--zacks-estimates-universe", action="store_true",
                    help="per-member EPS+Sales estimates snapshot for the current "
                         "Norgate universe (the idempotent 30-day forward-capture command)")
    ap.add_argument("--fundamentals-metadata", action="store_true")
    ap.add_argument("--fundamentals-tags", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--as-of", default=None, help="snapshot business date (default: today UTC)")
    ap.add_argument("--force", action="store_true", help="supersede an existing same-day snapshot")
    ap.add_argument("--rate", type=float, default=0.25,
                    help="min seconds between requests (default 0.25; raise if rate-limited)")
    ap.add_argument("--max-companies", type=int, default=None)
    args = ap.parse_args()

    cfg = _load_cfg()
    root = _research_root(cfg)
    if args.status:
        for name in ("metadata_checkpoint", "tags_checkpoint"):
            p = root / "fundamentals" / "checkpoints" / ("%s.json" % name)
            if p.is_file():
                ck = json.loads(p.read_text(encoding="utf-8"))
                states = {}
                for v in ck["done"].values():
                    states[v] = states.get(v, 0) + 1
                print("%s: %d done %s (updated %s)" % (name, len(ck["done"]),
                                                       json.dumps(states, sort_keys=True),
                                                       ck.get("updated_at")))
            else:
                print("%s: absent" % name)
        for feed in ZACKS_FEEDS:
            d = root / "zacks_snapshots" / feed
            manifests = sorted(d.glob("*.manifest.json")) if d.is_dir() else []
            print("zacks[%s]: %d snapshot(s) %s" % (feed, len(manifests),
                                                    [m.name for m in manifests[-3:]]))
        return 0

    c = build_collector(cfg, rate=args.rate)
    key = c._resolve_key()
    if key is None:
        print("INTRINIO_TRIAL_ACQUIRE = NOT_PROVISIONED (no credential; no network call)")
        return 2
    as_of = args.as_of or _dt.datetime.now(_dt.timezone.utc).date().isoformat()

    ran = False
    if args.zacks_snapshot:
        ran = True
        print("== Zacks full-cross-section snapshot as_of=%s ==" % as_of)
        summary = run_zacks_snapshot(c, key, root, as_of, force=args.force)
        print(json.dumps(summary, indent=1, sort_keys=True))
    if args.zacks_estimates_universe:
        ran = True
        print("== Zacks per-member estimates snapshot as_of=%s ==" % as_of)
        summary = run_zacks_estimates_universe(c, key, root, as_of, force=args.force)
        print(json.dumps(summary, indent=1, sort_keys=True))
    if args.fundamentals_metadata:
        ran = True
        print("== US Fundamentals metadata census (CIK-first) ==")
        counts = run_fundamentals_metadata(c, key, root, max_companies=args.max_companies)
        print(json.dumps(counts, indent=1, sort_keys=True))
    if args.fundamentals_tags:
        ran = True
        print("== US Fundamentals quarterly tag history (CIK-verified only) ==")
        counts = run_fundamentals_tags(c, key, root, max_companies=args.max_companies)
        print(json.dumps(counts, indent=1, sort_keys=True))
    if not ran:
        ap.print_help()
        return 1
    print("INTRINIO_TRIAL_ACQUIRE_DONE requests=%d errors=%d rate_limited=%d"
          % (c.requests_attempted, c.http_errors, c.rate_limited_hits))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
