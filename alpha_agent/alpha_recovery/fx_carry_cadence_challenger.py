r"""alpha_agent.alpha_recovery.fx_carry_cadence_challenger - the EXACT frozen FX carry
cadence record, resolved for a human-authorised TRUE_FORWARD registration.

WHAT THIS IS
    The global multi-asset frontier ranked dated-contract FX carry second and named
    one specification as its forward-evidence vehicle:
    ``ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7`` (score every eligible session,
    trade every 5, no-trade band 0.25). A human authorised its prospective
    registration on 2026-09-14.

    That record was written by ``alpha_agent.alpha_recovery.forward_package`` as an
    immutable file whose name carries its record hash. It is NOT a row in
    ResearchMemory, so the generic operator entrypoint (which resolves memory
    freezes) cannot name it. This module is the read side of the same seam the
    next-open challenger uses: it READS the immutable record, re-verifies both of
    its hashes, resolves the instrument universe from the R64 owner record of the
    same dataset, and returns a freeze row in the shape the canonical adoption owner
    reads. Nothing is reconstructed from prose and no parameter is chosen here.

WHAT THIS IS NOT
    Not a registrar, not a lifecycle rule, not an observation clock, not an emitter
    and not a scorer. It imports no application module. The registration itself is
    performed by ``scripts/register_fx_carry_cadence_challenger.py`` through the
    ONE governed adoption owner.

EVIDENCE
    The record is NOT qualified: ECONOMIC_UNDER_CONTROLS_NOT_FDR, family Holm FAIL,
    lockbox POST_SELECTION. Registration starts a measurement. It carries zero
    forward observations, backfills nothing, promotes nothing, makes nothing
    capital-eligible, allocates nothing and creates no order.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from alpha_agent import r64 as _r64

from . import reversed_skew as RS
from . import research_root, stable_hash

CALCULATION_OWNER = "alpha_agent.alpha_recovery.fx_carry_cadence_challenger"

CHALLENGER_ID = "ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7"
#: The immutable record's own hashes, verified on every read. A record whose bytes
#: no longer hash to these is refused, never repaired.
RECORD_HASH = "b4016c359e4952a347b917984a208242678409191056b1be85d399fed97c7f1f"
FREEZE_RECORD_HASH = "6cfd2ed99dccfdc069b159147c140e904674614ce26355f33bc4e3b171bb021a"
RECORD_FILE = "%s_%s.json" % (CHALLENGER_ID, RECORD_HASH[:12])
RECORD_SUBDIR = "challengers"

#: The frozen cadence the global frontier named. Read back from the record and
#: checked, never used to build anything.
FROZEN_CELL_ID = "FX_FUTURES|XS|1|CARRY|k5|b0.25"
FROZEN_TRADE_EVERY = 5
FROZEN_NO_TRADE_BAND = 0.25
FROZEN_HORIZON = 1

#: The same release label as every Alpha Recovery registration, so the canonical
#: accrual owner resolves this challenger's decisions from the campaign's own
#: decision owner and from nowhere else.
RELEASE = RS.RELEASE
VENUE = "CME"

#: The record was written by the forward package build whose artifact is stamped
#: 2026-09-10T15:07:32Z (results/forward_candidates.json). Pinned, because a later
#: rebuild restamps that artifact and the identity must not move with it.
INCEPTION = "2026-09-10"

#: The book label of the construction that measured the record, as the cadence
#: owner spells it (``cadence.build_book_cadence``); a test pins the spelling.
MODEL_FAMILY = "XS_LONG_SHORT_RISK_CONTROLLED_CADENCE_%d" % FROZEN_TRADE_EVERY

#: The instrument universe of the dataset the cadence cell was scored on, as the
#: R64 owner recorded it for the same scope, mode and horizon.
UNIVERSE_SOURCE_GLOB = "R64_FX_FUTURES_CARRY_H1_E82A5C66_*.json"
CADENCE_CELL_FILE = "FX_FUTURES_XS_1_CARRY_k5_b0.25.json"

REGISTRATION_OWNER = "scripts/register_fx_carry_cadence_challenger.py"
REGISTRATION_ARTIFACT = "fx_carry_cadence_registration.json"

AUTHORISATION = {
    "authorised_by": "human operator",
    "authorised_on": "2026-09-14",
    "instruction": "PAPER TRADER - AUTHORIZE FX TRUE_FORWARD + COMPLETE END-TO-END ALPHA AGENT AUTONOMY",
    "approved": "prospective TRUE_FORWARD evidence collection for this exact frozen identity only",
    "not_approved": ["model promotion", "capital eligibility", "portfolio allocation",
                     "portfolio mutation", "an order", "an execution instruction"],
}

EVIDENCE_AT_INCEPTION = {
    "historical_classification": "ECONOMIC_UNDER_CONTROLS_NOT_FDR",
    "qualified": False,
    "family_holm": "FAIL",
    "lockbox": "POST_SELECTION",
    "historical_result_is_forward_evidence": False,
    "true_forward_observations": 0,
    "backfilled": False,
    "capital_eligible": False,
}

#: What the canonical accrual owner needs before this registration can emit, and
#: does not yet have. Stated so a registration is never mistaken for accrual.
FORWARD_EMISSION_REQUIREMENT = (
    "api.canonical_forward_accrual resolves an ALPHA_RECOVERY_OFFENSIVE challenger's book only "
    "from decisions frozen per session by alpha_agent.alpha_recovery.prospective_decision under "
    "a declared emission policy. No FX carry cadence decision emitter or policy exists, so the "
    "registration accrues no observation until one is built and declared; nothing is ever "
    "backfilled for sessions that pass before then.")

SAFETY = {
    "research_only": True,
    "paper_only": True,
    "promotes_model": False,
    "capital_eligible": False,
    "allocates_capital": False,
    "mutates_portfolio": False,
    "creates_orders": False,
    "creates_fills": False,
    "backfill_allowed": False,
    "automatic_promotion": False,
    "manual_review_required": True,
}


def record_dir() -> Path:
    return research_root() / RECORD_SUBDIR


def _read(path: Path) -> Optional[dict]:
    try:
        out = json.loads(Path(path).read_text(encoding="utf-8"))
        return out if isinstance(out, dict) else None
    except (OSError, ValueError):
        return None


def verify_record(rec: Optional[dict], *, expected_record_hash: Optional[str] = None,
                  expected_freeze_record_hash: Optional[str] = None) -> list:
    """Every reason this record is not the exact frozen identity. Pure."""
    if not isinstance(rec, dict):
        return ["RECORD_MISSING"]
    expected_record_hash = expected_record_hash or RECORD_HASH
    expected_freeze_record_hash = expected_freeze_record_hash or FREEZE_RECORD_HASH
    problems = []
    body = {k: v for k, v in rec.items() if k not in ("record_hash", "record_file")}
    if rec.get("record_hash") != expected_record_hash:
        problems.append("RECORD_HASH_NOT_PINNED: %s" % rec.get("record_hash"))
    if stable_hash(body) != rec.get("record_hash"):
        problems.append("RECORD_BYTES_DO_NOT_HASH_TO_THE_RECORDED_HASH")
    spec = rec.get("forward_specification") or {}
    if rec.get("freeze_record_hash") != expected_freeze_record_hash:
        problems.append("FREEZE_RECORD_HASH_NOT_PINNED: %s" % rec.get("freeze_record_hash"))
    if stable_hash(spec) != rec.get("freeze_record_hash"):
        problems.append("SPECIFICATION_DOES_NOT_HASH_TO_THE_FREEZE_RECORD_HASH")
    if rec.get("challenger_id") != CHALLENGER_ID:
        problems.append("CHALLENGER_ID_MISMATCH: %s" % rec.get("challenger_id"))
    checks = (("cell_id", spec.get("cell_id"), FROZEN_CELL_ID),
              ("trade_every_sessions", spec.get("trade_every_sessions"), FROZEN_TRADE_EVERY),
              ("no_trade_band", spec.get("no_trade_band"), FROZEN_NO_TRADE_BAND),
              ("horizon_sessions", spec.get("horizon_sessions"), FROZEN_HORIZON))
    for name, got, want in checks:
        if got != want:
            problems.append("FROZEN_%s_MISMATCH: %r != %r" % (name.upper(), got, want))
    if rec.get("promotion_allowed") is not False or rec.get("live_registration_performed") is not False:
        problems.append("RECORD_SAFETY_FLAGS_CHANGED")
    return problems


def resolve_universe(*, universe_dir: Optional[Path] = None,
                     cells_dir: Optional[Path] = None) -> dict:
    """The instrument scope, read from the R64 owner record of the same dataset."""
    d = Path(universe_dir) if universe_dir else _r64.research_root() / RECORD_SUBDIR
    lists = []
    sources = []
    for p in (sorted(d.glob(UNIVERSE_SOURCE_GLOB)) if d.exists() else []):
        rec = _read(p) or {}
        # The R64 owner records the universe as {"instruments": [...], ...}.
        uni = (rec.get("universe") or {}).get("instruments") if isinstance(rec.get("universe"), dict) \
            else rec.get("universe")
        if isinstance(uni, list) and uni:
            lists.append([str(x) for x in uni])
            sources.append(str(p))
    if not lists:
        return {"ok": False, "reason": "UNIVERSE_SOURCE_MISSING: no %s under %s"
                                       % (UNIVERSE_SOURCE_GLOB, d)}
    if any(x != lists[0] for x in lists):
        return {"ok": False, "reason": "UNIVERSE_SOURCES_DISAGREE", "sources": sources}
    cell = _read(Path(cells_dir or research_root() / "cells") / CADENCE_CELL_FILE)
    n = (cell or {}).get("n_instruments")
    if cell is not None and n != len(lists[0]):
        return {"ok": False, "reason": "UNIVERSE_SIZE_DISAGREES_WITH_THE_CADENCE_CELL: %r != %d"
                                       % (n, len(lists[0]))}
    return {"ok": True, "instruments": lists[0], "sources": sources,
            "cadence_cell_n_instruments": n}


def resolve(*, directory: Optional[Path] = None, universe_dir: Optional[Path] = None,
            cells_dir: Optional[Path] = None) -> dict:
    """The verified record and universe, or every reason it cannot be registered."""
    path = Path(directory or record_dir()) / RECORD_FILE
    rec = _read(path)
    problems = verify_record(rec)
    uni = resolve_universe(universe_dir=universe_dir, cells_dir=cells_dir)
    if not uni.get("ok"):
        problems.append(uni.get("reason"))
    return {"ok": not problems, "problems": problems, "record_path": str(path),
            "record": rec, "universe": uni}


def cost_model(rec: dict) -> dict:
    spec = rec.get("forward_specification") or {}
    return {"rule": spec.get("costs"),
            "per_market_bps_per_side_owner": "alpha_agent.r64.experiments.assemble "
                                             "(R38 market meta cost_bps_per_side)",
            "applied": "one-way turnover at every rebalance",
            "stress_evidence": "qualification_evidence.cadence_economics.ann_net_increment_at_2x_cost"}


def freeze_row(resolved: Optional[dict] = None) -> dict:
    """The immutable freeze, in the shape the canonical adoption owner reads."""
    res = resolved or resolve()
    if not res.get("ok"):
        raise ValueError("the frozen FX carry cadence record cannot be registered: %s"
                         % "; ".join(res.get("problems") or []))
    rec = res["record"]
    spec = rec["forward_specification"]
    instruments = list(res["universe"]["instruments"])
    return {
        "hypothesis_id": RECORD_FILE[:-len(".json")],
        "release": RELEASE,
        "asset_class": rec["asset_class"],
        "economic_family": (rec.get("information_identity") or {}).get("family"),
        "model_family": MODEL_FAMILY,
        "horizon_sessions": int(rec["horizon_sessions"]),
        "outcome": "FORWARD_FROZEN",
        "invalidated_reason": None,
        "input_data_identity": stable_hash({"record_file": RECORD_FILE, "record_hash": RECORD_HASH,
                                            "universe_sources": [Path(s).name for s in
                                                                 res["universe"]["sources"]],
                                            "cell_id": spec.get("cell_id")}),
        "spec_json": {**spec, "instrument_scope": instruments, "venue": VENUE,
                      "sleeve": (rec.get("information_identity") or {}).get("family"),
                      "cost_model": cost_model(rec), "feature_snapshot_hash": RECORD_HASH},
        "forward_challenger": {"challenger_id": CHALLENGER_ID,
                               "record_hash": FREEZE_RECORD_HASH,
                               "inception": INCEPTION},
    }


__all__ = ["CALCULATION_OWNER", "CHALLENGER_ID", "RECORD_HASH", "FREEZE_RECORD_HASH", "RECORD_FILE",
           "RELEASE", "VENUE", "INCEPTION", "MODEL_FAMILY", "AUTHORISATION", "EVIDENCE_AT_INCEPTION",
           "FORWARD_EMISSION_REQUIREMENT", "SAFETY", "verify_record", "resolve_universe", "resolve",
           "cost_model", "freeze_row", "REGISTRATION_OWNER", "REGISTRATION_ARTIFACT"]
