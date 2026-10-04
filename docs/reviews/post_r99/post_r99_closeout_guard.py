r"""post_r99_closeout_guard.py - run the CANONICAL R99 contract guard at closeout without fabricating active time.

The R99 clock rule (R99_START_STATE.json) is "wall clock since started_at_utc minus any interval recorded in
idle_exclusions". The last R99 work-log entry and the last guard run are both 2026-10-04T17:54:18Z. Everything
after that instant is a post-campaign review (this session), not R99 research. Without an exclusion, any later
guard run would count that review time as research and could mechanically reach H4 (>= 180 active minutes) -
i.e. a hard stop granted on fabricated active minutes.

This script, in one process and at one instant `now`:
  1. copies R99_START_STATE.json and R99_CONTRACT_GUARD.json into this review folder (before-closeout copies);
  2. appends ONE idle exclusion [17:54:18Z, now] with its reason (the clock rule's own mechanism);
  3. runs r99_contract_guard.run(d, now) unchanged - the guard writes its own R99_CONTRACT_GUARD.json.

It changes no quota, no counter source, no result and no verdict. Idempotent: a second run refuses if a
post-R99 exclusion already exists.
"""
from __future__ import annotations

import importlib.util
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
R99 = HERE.parents[2] / "research" / "agents" / "campaign_r99_multi_asset_alpha_offensive"
LAST_R99_ACTIVITY = "2026-10-04T17:54:18Z"   # last R99_ACTIVE_WORK_LOG entry == last guard timestamp
TAG = "POST_R99_CLOSEOUT_REVIEW"


def main() -> dict:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    st_path = R99 / "R99_START_STATE.json"
    st = json.loads(st_path.read_text(encoding="utf-8-sig"))
    if any(x.get("tag") == TAG for x in st.get("idle_exclusions") or []):
        raise SystemExit("REFUSED: a %s idle exclusion is already recorded" % TAG)
    log_last = [json.loads(x) for x in (R99 / "R99_ACTIVE_WORK_LOG.jsonl").read_text(encoding="utf-8-sig").splitlines() if x.strip()][-1]
    if log_last["timestamp"] != LAST_R99_ACTIVITY:
        raise SystemExit("REFUSED: the R99 work log moved (%s); re-derive the idle interval" % log_last["timestamp"])
    shutil.copyfile(st_path, HERE / "R99_START_STATE.before_closeout.json")
    shutil.copyfile(R99 / "R99_CONTRACT_GUARD.json", HERE / "R99_CONTRACT_GUARD.before_closeout.json")
    t0 = datetime.strptime(LAST_R99_ACTIVITY, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    st.setdefault("idle_exclusions", []).append({
        "tag": TAG, "from_utc": LAST_R99_ACTIVITY, "to_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "minutes": round((now - t0).total_seconds() / 60.0, 3),
        "reason": "No R99 research after the last R99 work-log entry. The interval is a post-campaign review "
                  "(independent recount, pipeline repair, architecture review) and must not count as R99 active "
                  "research minutes.",
        "recorded_by": "docs/reviews/post_r99/post_r99_closeout_guard.py"})
    st_path.write_text(json.dumps(st, indent=1), encoding="utf-8")
    spec = importlib.util.spec_from_file_location("r99_contract_guard", R99 / "r99_contract_guard.py")
    g = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(g)
    rec = g.run(R99, now=now, write=True)
    return rec


if __name__ == "__main__":
    r = main()
    print(json.dumps({k: r[k] for k in ("timestamp_utc", "reason", "hard_stop_events_valid", "pending_downstream")}, indent=1))
    print("ACTIVE_RESEARCH_MINUTES", r["counters"]["ACTIVE_RESEARCH_MINUTES"])
    print(r["decision"])
