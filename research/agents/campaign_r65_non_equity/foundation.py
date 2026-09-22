r"""R65 foundation - extend aggregate open interest to the FULL certified book.

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION. NEW_PAID_DATA_COST
= $0: every byte comes from the locally installed Norgate 'Futures' database the
estate already owns, through the frozen R38 contract registry.

    & .\.venv-win\Scripts\python.exe research\agents\campaign_r65_non_equity\foundation.py --build
    & .\.venv-win\Scripts\python.exe research\agents\campaign_r65_non_equity\foundation.py --certify

The gap this closes
-------------------
``campaign_r56_v2.data_r38`` computes, per market and session, how many listed
delivery months exist and how many of them carry POSITIVE open interest
(``n_contracts_summed`` / ``n_contracts_oi_positive``). Both are written to the
certified CSVs. But ``load_aggregate_oi`` returns only the first of the two, and
the dataset was only ever built for the 39 COMMODITY markets of the R56 panel.

So a curve-participation mechanism - what fraction of the listed curve is
actually warehousing risk - is unaskable today, not because the estate does not
own the data, but because 29 of its 68 certified markets were never built and
the column that carries the numerator is dropped at load.

That matters for POWER, which is the whole point. A commodity-only panel carries
the estate's WORST measured detection floor (MDE_80 6.56%/yr net at burden 1,
10.63%/yr at burden 10). The full cross-asset book carries its BEST (2.98%/yr at
burden 1, 4.19%/yr at burden 10). Asking a new question on the commodity panel
alone is asking it where it cannot be answered.

What this module does, and what it refuses to do
------------------------------------------------
It builds a SECOND, SEPARATE dataset for the 29 markets the R56 dataset does not
carry, using ``data_r38.aggregate_market`` - IMPORTED, never reimplemented, so
there is one definition of what "aggregate open interest" means.

It NEVER writes into ``r38_aggregate_oi_v1``. That dataset is the certified
foundation of a settled campaign; appending to it would silently change the
substrate a settled result was measured on.

The loader :func:`cross_asset_curve_panel` merges the two and records, per
market, WHICH dataset and WHICH vendor vintage each row came from. The two were
built at different vintages and a reader is entitled to know that.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[2]
for _p in (str(_REPO), str(_HERE.parent)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from campaign_r56_v2 import data_r38 as D  # noqa: E402

DATASET_ID = "r65_aggregate_oi_cross_asset_v1"
OUT_DIR = (Path(r"D:\Stock_Prediction_app_data\r59_autonomous_alpha")
           / "agents_v2" / "campaign_R65_NON_EQUITY" / "foundation" / "data"
           / DATASET_ID)
MANIFEST = OUT_DIR / "manifest.json"

#: The curve-participation numerator the R56 loader drops.
DEPTH_NUM = "n_contracts_oi_positive"
DEPTH_DEN = "n_contracts_summed"


def missing_markets() -> list:
    """Certified markets with no row in the R56 aggregate-OI dataset."""
    layer = D.load_certified_layer(verify=False)
    man = json.loads(D.AGG_OI_MANIFEST.read_text(encoding="utf-8"))
    have = {m for m, r in (man.get("markets") or {}).items()
            if (r or {}).get("state") == "OK"}
    return [s for s in layer["symbols"] if s not in have]


def build(markets=None) -> dict:
    """Build the extension dataset. Writes ONLY under :data:`OUT_DIR`."""
    if D.AGG_OI_DIR.resolve() == OUT_DIR.resolve():
        raise RuntimeError(
            "refusing to write into the frozen R56 aggregate-OI dataset")
    want = list(markets) if markets else missing_markets()
    man = D.build_aggregate_oi(want, out_dir=OUT_DIR)
    man["dataset_id"] = DATASET_ID
    man["extends"] = D.AGG_OI_DATASET_ID
    man["extension_reason"] = (
        "the R56 dataset covers only the 39 COMMODITY markets of that "
        "campaign's panel; these %d markets complete the certified book so a "
        "curve-participation mechanism can be asked at the CROSS_ASSET "
        "detection floor instead of the commodity one" % len(want))
    man["frozen_dataset_untouched"] = str(D.AGG_OI_DIR)
    MANIFEST.write_text(json.dumps(man, indent=1), encoding="utf-8")
    return man


def _sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def cross_asset_curve_panel(layer: dict = None, *, verify: bool = True) -> dict:
    """Curve depth over the FULL certified book, from both datasets.

    Returns arrays on the layer's ``[market, session]`` grid:

        n_oi_positive   listed delivery months carrying POSITIVE open interest
        n_listed        listed delivery months with an open-interest print
        depth           n_oi_positive / n_listed, NaN where n_listed is 0

    UNLAGGED, exactly like the R56 loader: session s is index s, and a decision
    at t may read columns ``<= t - latest_usable_offset`` only.
    """
    import pandas as pd

    layer = layer if layer is not None else D.load_certified_layer(verify=False)
    dix = {d: i for i, d in enumerate(layer["dates"])}
    shape = (len(layer["symbols"]), len(layer["dates"]))
    num = np.full(shape, np.nan)
    den = np.full(shape, np.nan)
    provenance: dict = {}

    sources = [(D.AGG_OI_DIR, D.AGG_OI_MANIFEST)]
    if MANIFEST.exists():
        sources.append((OUT_DIR, MANIFEST))

    for directory, manifest_path in sources:
        man = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        vintage = man.get("vendor_database_vintage")
        for i, sym in enumerate(layer["symbols"]):
            row = (man.get("markets") or {}).get(sym)
            if not row or row.get("state") != "OK" or sym in provenance:
                continue
            path = Path(directory) / ("%s.csv" % sym)
            if verify and row.get("sha256") and _sha256(path) != row["sha256"]:
                raise RuntimeError("aggregate OI hash mismatch: %s" % sym)
            df = pd.read_csv(path)
            if DEPTH_NUM not in df.columns:
                provenance[sym] = {"dataset": man.get("dataset_id"),
                                   "state": "NO_DEPTH_COLUMN"}
                continue
            ii = np.array([dix.get(d, -1)
                           for d in df["Date"].astype(str).str[:10]])
            ok = ii >= 0
            num[i, ii[ok]] = df[DEPTH_NUM].to_numpy(np.float64)[ok]
            den[i, ii[ok]] = df[DEPTH_DEN].to_numpy(np.float64)[ok]
            provenance[sym] = {"dataset": man.get("dataset_id"),
                               "vendor_database_vintage": vintage,
                               "rows": int(ok.sum())}

    with np.errstate(invalid="ignore", divide="ignore"):
        depth = np.where(den > 0, num / den, np.nan)
    return {"n_oi_positive": num, "n_listed": den, "depth": depth,
            "provenance": provenance,
            "latest_usable_offset": D.OI_LATEST_USABLE_OFFSET,
            "datasets": [m.get("dataset_id") for _d, p in sources
                         for m in [json.loads(Path(p).read_text(
                             encoding="utf-8"))]],
            "unlagged": True}


def certify(start: str = "2011-07-01") -> dict:
    """What a data-foundation agent needs to rule on this substrate."""
    from alpha_agent import r59

    layer = D.load_certified_layer(verify=False)
    panel = cross_asset_curve_panel(layer, verify=True)
    dates = np.asarray(layer["dates"])
    t0 = int(np.searchsorted(dates, start))
    own = np.isfinite(layer["ret"])[:, t0:]
    den = panel["n_listed"][:, t0:]
    usable = own & np.isfinite(den) & (den > 0)

    by_class: dict = {}
    for i, sym in enumerate(layer["symbols"]):
        ac = layer["asset_class"][i]
        d = by_class.setdefault(ac, {"markets": 0, "covered": 0,
                                     "eligible": 0, "with_data": []})
        d["markets"] += 1
        d["eligible"] += int(own[i].sum())
        d["covered"] += int(usable[i].sum())
        if usable[i].any():
            d["with_data"].append(sym)
    for ac, d in by_class.items():
        d["coverage"] = (d["covered"] / d["eligible"]) if d["eligible"] else None
        d["markets_with_data"] = len(d.pop("with_data"))

    return {
        "dataset_id": DATASET_ID,
        "extends": D.AGG_OI_DATASET_ID,
        "new_paid_data_cost_usd": 0.0,
        "source": "locally installed Norgate 'Futures' database (OWNED)",
        "start": start,
        "certified_markets": len(layer["symbols"]),
        "markets_with_curve_depth": int(sum(
            1 for i in range(len(layer["symbols"])) if usable[i].any())),
        "overall_coverage": (float(usable.sum() / own.sum())
                             if own.sum() else None),
        "by_asset_class": by_class,
        "provenance": panel["provenance"],
        "availability_rule":
            "exchange open interest for session s is observable on s+1; a "
            "decision at index t reads columns <= t-%d"
            % D.OI_LATEST_USABLE_OFFSET,
        "vintage_caveat":
            "the two datasets were built at different vendor database "
            "vintages; per-market provenance records which produced each row",
        "safety": ["RESEARCH ONLY", "PAPER ONLY", "NO ORDERS",
                   "NO PURCHASE", "PREVIEW ONLY"],
        "r59_constants": {"discovery_start": r59.DISCOVERY_START,
                          "validation_start": r59.VALIDATION_START,
                          "lockbox_start": r59.LOCKBOX_START},
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--certify", action="store_true")
    ap.add_argument("--out", default="")
    args = ap.parse_args(argv)
    try:
        if args.build:
            man = build()
            ok = sum(1 for r in man["markets"].values()
                     if r.get("state") == "OK")
            print(json.dumps({"dataset_id": man["dataset_id"],
                              "markets_built": ok,
                              "markets_attempted": len(man["markets"]),
                              "vendor_database_vintage":
                                  man.get("vendor_database_vintage"),
                              "out_dir": str(OUT_DIR),
                              "detail": man["markets"]},
                             indent=1, default=str))
        if args.certify:
            body = certify()
            if args.out:
                Path(args.out).write_text(
                    json.dumps(body, indent=1, default=str), encoding="utf-8")
            print(json.dumps(body, indent=1, default=str))
        print("R65_FOUNDATION_OK")
        return 0
    except Exception as exc:                                   # noqa: BLE001
        print(json.dumps({"failed": type(exc).__name__,
                          "detail": str(exc)[:900]}, indent=1))
        print("R65_FOUNDATION_FAILED %s" % type(exc).__name__)
        return 1


if __name__ == "__main__":
    sys.exit(main())
