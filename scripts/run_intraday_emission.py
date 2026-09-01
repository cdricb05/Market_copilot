r"""ONE bounded intraday emission attempt (Release 53.1).

The single entrypoint for the intraday slot clock - today invoked manually,
later by a dedicated Scheduled Task the OPERATOR installs. Each invocation:

1. re-probes the canonical intraday-lane owner (``alpha_agent.r46.intraday``)
   LIVE, so the factory's gate always reflects measured reality;
2. builds ONE normalized feed snapshot through the canonical market-data
   owner (``engine.market_data``);
3. scores every matured prediction first (same-feed marks, MATURED only);
4. attempts ONE emission for the current slot through the FROZEN R53 factory
   (``alpha_agent.r53.intraday_factory.emit_due``) - outside a legal slot the
   factory refuses and that refusal is the result;
5. records the attempt in an idempotent attempts sidecar (so a forfeiture
   sweep can distinguish "no invocation happened" from "an invocation
   produced zero signals") and writes the emission-status artifact.

It NEVER: backdates, force-emits outside a slot, retunes a frozen spec,
touches a production store, an order, a scheduler, or the portfolio.

Exit tokens (printed, single line, canonical convention):
    R53_1_INTRADAY_EMISSION <EMITTED|NOT_AN_EMISSION_SLOT|LANE_BLOCKED_STRUCTURAL|...>
"""
from __future__ import annotations

import datetime as _dt
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent.r46 import intraday as lane_owner       # noqa: E402
from paper_trader.alpha_agent.r53 import intraday_factory as factory  # noqa: E402
from paper_trader.alpha_agent import r53_1 as R                       # noqa: E402
from paper_trader.alpha_agent.r53_1 import intraday_feed as feed      # noqa: E402
from paper_trader.alpha_agent.r53_1 import intraday_signals as sigs   # noqa: E402

ATTEMPTS_FILE = "r53_1_emission_attempts.json"
ARTIFACT = "R53_1_INTRADAY_EMISSION_STATUS.json"


def _attempts_path() -> Path:
    return factory.ledger_dir() / ATTEMPTS_FILE


def record_attempt(slot_or_state: str, now_iso: str, result: dict) -> None:
    p = _attempts_path()
    rows = []
    if p.exists():
        rows = json.loads(p.read_text(encoding="utf-8"))
    rows.append({"at_utc": now_iso, "slot_or_state": slot_or_state,
                 "emit_state": result.get("state"),
                 "n_appended": result.get("n_appended", 0)})
    p.write_text(json.dumps(rows, indent=1), encoding="utf-8")


def main() -> int:
    t0 = time.time()
    now = _dt.datetime.now(_dt.timezone.utc)
    now_iso = now.isoformat().replace("+00:00", "Z")

    lane = lane_owner.probe(live_probe=True)
    t_lane = time.time()

    snapshot = feed.build_snapshot(now_utc=now)
    t_snap = time.time()

    scored = factory.score_due(now_utc=now,
                               mark_fn=feed.make_mark_fn(snapshot))
    t_score = time.time()

    result = factory.emit_due(
        now_utc=now,
        signal_fn=sigs.make_signal_fn(snapshot),
        session_close_utc=snapshot["session_close_utc"])
    t_emit = time.time()

    slot = (result.get("slot") or {}).get("slot_utc") or result.get("state")
    record_attempt(str(slot), now_iso, result)

    body = R.artifact_body(
        "r53_1_intraday_emission_status/1",
        "scripts.run_intraday_emission",
        release=R.RELEASE, campaign_id=R.CAMPAIGN_ID,
        attempted_at_utc=now_iso,
        lane_state=lane.get("state"),
        lane_sources={s["source"]: {"state": s.get("state"),
                                    "latency_class": s.get("latency_class"),
                                    "delay_seconds":
                                        s.get("measured_delay_seconds")}
                      for s in (lane.get("sources") or [])},
        feed={"provider": snapshot.get("provider"),
              "instruments_served": sorted(snapshot["bars"].keys()),
              "failures": snapshot["failures"],
              "freshness_seconds": snapshot["freshness_seconds"],
              "latency_class": feed.snapshot_latency_class(snapshot)},
        emission={k: v for k, v in result.items() if k != "lane"},
        scoring={k: v for k, v in scored.items()
                 if k in ("state", "n_scored", "n_unmarkable_still_pending")},
        ledger_totals={"predictions": len(factory.predictions()),
                       "outcomes": len(factory.outcomes()),
                       "forfeitures": len(factory.forfeitures())},
        latency_seconds={
            "lane_probe": round(t_lane - t0, 2),
            "feed_snapshot": round(t_snap - t_lane, 2),
            "outcome_scoring": round(t_score - t_snap, 2),
            "emission": round(t_emit - t_score, 2),
            "total": round(t_emit - t0, 2)},
        **R.safety_block(),
    )
    R.write_json(R.research_dir() / ARTIFACT, body)
    factory.write_factory_artifact()

    print("R53_1_INTRADAY_EMISSION %s appended=%s scored=%s total_s=%.1f"
          % (result.get("state"), result.get("n_appended", 0),
             scored.get("n_scored", 0), t_emit - t0))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
