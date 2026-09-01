r"""scripts/r53_store_hash.py - Release 53 production-store integrity snapshot.

Hashes every decision-relevant PRODUCTION store byte-for-byte so the release can
prove it mutated nothing operational. Research stores (R46 tournament, R52
runtime, R53 evidence) are deliberately excluded: research owners are EXPECTED
to write there.

Read-only. Writes exactly one JSON snapshot into the path given as argv[1].

Usage (Windows PowerShell):
    .venv-win\Scripts\python.exe scripts\r53_store_hash.py <out.json> <label>
"""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

#: The decision-relevant production stores. A change to ANY byte below would be
#: a production mutation Release 53 is forbidden to make.
PRODUCTION_STORES = [
    Path.home() / ".paper_trader" / "paper_trading_desk",
    Path.home() / ".paper_trader" / "current_alpha_paper_book",
    Path(r"D:\Stock_Prediction_app_data\portfolio_decisions"),
    Path(r"D:\Stock_Prediction_app_data\reallocation_proposals"),
    Path(r"D:\Stock_Prediction_app_data\portfolio_reassessments"),
    Path(r"D:\Stock_Prediction_app_data\rebalance_order_plans"),
    Path(r"D:\Stock_Prediction_app_data\holding_opportunity_cost"),
    Path(r"D:\Stock_Prediction_app_data\reassessment_outcomes"),
    Path(r"D:\Stock_Prediction_app_data\corporate_actions"),
]


def _file_sha(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def snapshot() -> dict:
    files = {}
    for root in PRODUCTION_STORES:
        if not root.exists():
            files[str(root)] = "ABSENT"
            continue
        for p in sorted(root.rglob("*")):
            if p.is_file():
                files[str(p)] = _file_sha(p)
    combined = hashlib.sha256(
        json.dumps(files, sort_keys=True).encode("utf-8")).hexdigest()
    return {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "file_count": sum(1 for v in files.values() if v != "ABSENT"),
        "combined_sha256": combined,
        "files": files,
    }


def main() -> int:
    out = Path(sys.argv[1])
    label = sys.argv[2] if len(sys.argv) > 2 else "snapshot"
    body = snapshot()
    body["label"] = label
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(body, indent=2), encoding="utf-8")
    print("R53_STORE_HASH %s combined=%s files=%d"
          % (label, body["combined_sha256"], body["file_count"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
