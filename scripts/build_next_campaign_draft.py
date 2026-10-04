r"""build_next_campaign_draft.py - the next-campaign draft as a PURE PROJECTION of one carry-forward record.

RESEARCH ONLY. PAPER ONLY. Writes one JSON file; reads one. No research-memory write, no preregistration.

R99 lesson: R99_NEXT_CAMPAIGN_DRAFT.json was hand-copied at 13:34 and kept "~3.4 years" for R99_X04 after
the authoritative R99_CARRY_FORWARD.json (13:52) had superseded it with 4.5 years at the observed rate.
A draft that is generated cannot disagree with its source: every item here is copied from the carry-forward
record, numeric timelines are RE-DERIVED from their inputs and checked against the recorded value, and the
draft carries the sha256 of the record it was built from.

    & .\.venv-win\Scripts\python.exe scripts\build_next_campaign_draft.py <CARRY_FORWARD.json> <out.json> [--check <old_draft.json>]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

GENERATOR = "scripts/build_next_campaign_draft.py"
SUPERSEDED_SUFFIX = "_SUPERSEDED"


def _load(p: Path):
    return json.loads(Path(p).read_text(encoding="utf-8-sig"))


def _walk(o, path=()):
    if isinstance(o, dict):
        for k, v in o.items():
            yield path + (k,), k, v
            yield from _walk(v, path + (k,))
    elif isinstance(o, list):
        for i, v in enumerate(o):
            yield from _walk(v, path + (i,))


def derive_event_accrual(evidence: dict) -> list:
    """Re-derive every 'years to the lockbox floor' an evidence block records from its own inputs."""
    out = []
    for name, ev in (evidence or {}).items():
        if not isinstance(ev, dict):
            continue
        need = ("lockbox_merged_obs_now", "floor", "meetings_per_year_since_2024", "observed_lockbox_nonzero_share")
        if not all(ev.get(k) is not None for k in need):
            continue
        rate = float(ev["meetings_per_year_since_2024"]) * float(ev["observed_lockbox_nonzero_share"])
        years = (float(ev["floor"]) - float(ev["lockbox_merged_obs_now"])) / rate if rate > 0 else None
        rec = ev.get("expected_years_to_floor_observed_rate")
        out.append({"evidence": name, "derived_years_to_floor": None if years is None else round(years, 2),
                    "recorded_years_to_floor": rec,
                    "consistent": (years is not None and rec is not None and abs(round(years, 1) - float(rec)) <= 0.05)})
    return out


def superseded_values(record: dict) -> list:
    return [{"path": "/".join(map(str, p)), "value": v} for p, k, v in _walk(record)
            if isinstance(k, str) and k.endswith(SUPERSEDED_SUFFIX) and isinstance(v, (int, float, str))]


def stale_facts(other_draft: dict, record: dict) -> list:
    """Superseded values that a (hand-written) draft still states next to a unit word."""
    text = json.dumps(other_draft, ensure_ascii=False)
    out = []
    for s in superseded_values(record):
        val = re.escape(str(s["value"]))
        for m in re.finditer(r"[^\"]{0,80}(?<![\d.])%s(?![\d.])\s*(year|yr|%%|bp)[^\"]{0,40}" % val, text):
            out.append({"superseded": s, "stale_text": m.group(0).strip()})
    return out


def build(carry_forward_path) -> dict:
    p = Path(carry_forward_path)
    cf = _load(p)
    acc = derive_event_accrual(cf.get("added_evidence"))
    return {
        "artifact": "NEXT_CAMPAIGN_DRAFT",
        "status": "DRAFT - generated, not preregistered. A new campaign id, a fresh declared cap, an immutable "
                  "code snapshot, data certification and fresh director rulings are required before any return "
                  "is read.",
        "generator": GENERATOR,
        "generated_from": p.as_posix(),
        "source_artifact": cf.get("artifact"),
        "source_sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
        "ranking": cf.get("ranking_update_after_W11") or cf.get("ranking"),
        "director_agenda": cf.get("director_ranked_agenda"),
        "not_reopenable": cf.get("not_reopenable"),
        "evidence": cf.get("added_evidence"),
        "derived_checks": {"event_accrual": acc, "all_consistent": all(a["consistent"] for a in acc)},
        "superseded_values_not_to_reuse": superseded_values(cf),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("carry_forward")
    ap.add_argument("out")
    ap.add_argument("--check", help="a hand-written draft to scan for superseded facts")
    a = ap.parse_args(argv)
    draft = build(a.carry_forward)
    if a.check:
        draft["stale_facts_in_checked_draft"] = {"draft": Path(a.check).as_posix(),
                                                 "findings": stale_facts(_load(Path(a.check)), _load(Path(a.carry_forward)))}
    Path(a.out).write_text(json.dumps(draft, indent=1, ensure_ascii=False), encoding="utf-8")
    print("DRAFT_BUILT", draft["source_sha256"][:12], "consistent", draft["derived_checks"]["all_consistent"],
          "stale", len((draft.get("stale_facts_in_checked_draft") or {}).get("findings") or []))
    return 0 if draft["derived_checks"]["all_consistent"] else 2


if __name__ == "__main__":
    sys.exit(main())
