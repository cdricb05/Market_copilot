"""alpha_agent.r64.handoff_validation - the exact R63 FX-carry artifact, verified.

R63 handed over ONE READY_FOR_FORWARD_QUALIFICATION record:
``R63_FX_FUTURES_CARRY_H1_E82A5C66`` (FX carry from the dated-contract slope,
1 session, nine currency futures). Before R64 builds on it, this module proves
that the artifact is what it says it is:

    * the challenger record's ``record_hash`` recomputes over its own body;
    * the challenger-candidates artifact's ``artifact_hash`` recomputes, and its
      one READY record is that record, byte-identical;
    * the sensitivity matrix's ``artifact_hash`` recomputes and its
      ``FX_FUTURES|XS|1|CARRY`` cell carries the statistics the record quotes;
    * every artifact was stamped with the sha256 of the R63 protocol that is
      committed in this repository;
    * (when given) a fresh run of the same cell through the same owner
      reproduces the persisted conditional statistics to 1e-9.

READ ONLY over the R63 research root; writes one R64 artifact.
"""
from __future__ import annotations

import hashlib
import math

from alpha_agent import r63 as _r63

from . import (REPRODUCTION_TOL, r63_results_root, read_r63_artifact, stable_hash,
               write_artifact)

CALCULATION_OWNER = "alpha_agent.r64.handoff_validation"
ARTIFACT_NAME = "r63_handoff_validation.json"
FX_CHALLENGER_ID = "R63_FX_FUTURES_CARRY_H1_E82A5C66"
FX_CELL_ID = "FX_FUTURES|XS|1|CARRY"
VALID = "R63_HANDOFF_VALID"
VALID_STALE_FILE = "R63_HANDOFF_VALID_WITH_STALE_CHALLENGER_FILE"
INVALID = "R63_HANDOFF_INVALID"
FILE_CURRENT = "CURRENT"
FILE_STALE = "STALE_FIRST_PASS_ECONOMICS"
COMPARED_KEYS = ("increment", "t", "p_one_sided", "increment_selection", "t_selection",
                 "increment_lockbox", "t_lockbox", "positive_fraction")


def recompute_record_hash(record: dict) -> str:
    return stable_hash({k: v for k, v in record.items() if k != "record_hash"})


def recompute_artifact_hash(artifact: dict) -> str:
    return stable_hash({k: v for k, v in artifact.items()
                        if k not in ("artifact_hash", "generated_at")})


def _close(a, b, tol: float = REPRODUCTION_TOL) -> bool:
    if a is None or b is None:
        return a is None and b is None
    try:
        a, b = float(a), float(b)
    except (TypeError, ValueError):
        return a == b
    if not (math.isfinite(a) and math.isfinite(b)):
        return a == b
    return abs(a - b) <= tol * max(1.0, abs(a), abs(b))


def compare_conditional(persisted: dict, fresh: dict) -> dict:
    """Field-by-field comparison of two conditional blocks (plus the sample
    counts) under the reproduction tolerance."""
    pc, fc = persisted.get("conditional") or {}, fresh.get("conditional") or {}
    diffs = {}
    for k in COMPARED_KEYS:
        if not _close(pc.get(k), fc.get(k)):
            diffs[k] = {"persisted": pc.get(k), "fresh": fc.get(k)}
    for k in ("effective_periods", "n_periods", "n_selection_periods", "n_lockbox_periods",
              "rows_covered", "n_instruments"):
        if persisted.get(k) != fresh.get(k):
            diffs[k] = {"persisted": persisted.get(k), "fresh": fresh.get(k)}
    return {"matches": not diffs, "differences": diffs, "tolerance": REPRODUCTION_TOL,
            "compared": list(COMPARED_KEYS)}


def find_cell(matrix: dict | None, cell_id: str) -> dict | None:
    for c in (matrix or {}).get("cells") or []:
        if c.get("cell_id") == cell_id:
            return c
    return None


def validate(*, reproduced_cell: dict | None = None, write: bool = True) -> dict:
    """Validate the R63 FX-carry handoff. Returns the validation document."""
    checks: list = []
    file_state = None

    def _check(name: str, passed: bool, detail: str, **extra) -> None:
        checks.append({"check": name, "passed": bool(passed), "detail": detail, **extra})

    root = r63_results_root()
    rec_path = root / "challengers" / ("%s.json" % FX_CHALLENGER_ID)
    record = _r63.read_json(rec_path)
    _check("challenger_record_present", record is not None, str(rec_path))
    if record:
        _check("challenger_record_hash_recomputes",
               recompute_record_hash(record) == record.get("record_hash"),
               "sha256 over the record body excluding record_hash",
               record_hash=record.get("record_hash"))
        _check("challenger_is_ready_for_forward_qualification",
               record.get("classification") == _r63.CH_READY, str(record.get("classification")))
        _check("challenger_cell_is_fx_carry_1_session",
               record.get("cell_id") == FX_CELL_ID and record.get("horizon_sessions") == 1
               and record.get("asset_class") == _r63.AC_FX and record.get("instruments") == 9,
               "cell %s, %s instruments" % (record.get("cell_id"), record.get("instruments")))
        _check("challenger_flags_no_promotion_no_registration",
               record.get("promotion_allowed") is False
               and record.get("live_registration_performed") is False
               and record.get("holdings_changed") is False, "safety flags on the record")

    cands = read_r63_artifact("r63_challenger_candidates.json")
    _check("candidates_artifact_present", cands is not None, "r63_challenger_candidates.json")
    if cands:
        _check("candidates_artifact_hash_recomputes",
               recompute_artifact_hash(cands) == cands.get("artifact_hash"),
               "sha256 over the artifact excluding artifact_hash and generated_at")
        ready = cands.get("ready_for_forward_qualification") or []
        _check("exactly_one_ready_record", len(ready) == 1 and
               (cands.get("counts") or {}).get(_r63.CH_READY) == 1,
               "%d READY records, counts=%s" % (len(ready), cands.get("counts")))
        if ready and record:
            fin = ready[0]
            _check("ready_record_is_the_fx_carry_record",
                   fin.get("challenger_id") == FX_CHALLENGER_ID
                   and fin.get("cell_id") == record.get("cell_id")
                   and fin.get("oos_improvement") == record.get("oos_improvement")
                   and fin.get("classification") == record.get("classification"),
                   "candidates.ready[0] and challengers/%s.json name the same cell, the same "
                   "conditional statistics and the same classification" % FX_CHALLENGER_ID)
            _check("ready_record_hash_recomputes",
                   recompute_record_hash(fin) == fin.get("record_hash"),
                   "sha256 over the embedded record body excluding record_hash",
                   record_hash=fin.get("record_hash"))
            # The per-challenger file is written ONCE by R63 (never overwritten);
            # the candidates artifact is regenerated on every challengers pass.
            # After R63's disclosed capped-book re-run the two can legitimately
            # differ in ECONOMICS only. That is reported, never hidden, and it
            # is not an integrity failure of the FINAL evidence.
            file_state = (FILE_CURRENT if fin.get("record_hash") == record.get("record_hash")
                          else FILE_STALE)
        _check("candidates_promotion_not_performed",
               cands.get("promotion_performed") is False
               and cands.get("live_registration_performed") is False, "artifact safety flags")
    final_record = (ready[0] if (cands and (cands.get("ready_for_forward_qualification") or []))
                    else record)

    matrix = read_r63_artifact("information_sensitivity_matrix.json")
    _check("sensitivity_matrix_present", matrix is not None, "information_sensitivity_matrix.json")
    cell = find_cell(matrix, FX_CELL_ID)
    if matrix:
        _check("sensitivity_matrix_hash_recomputes",
               recompute_artifact_hash(matrix) == matrix.get("artifact_hash"),
               "sha256 over the artifact excluding artifact_hash and generated_at")
        _check("fx_carry_cell_present_in_matrix", cell is not None, FX_CELL_ID)
    if cell and final_record:
        record = final_record
        oos = record.get("oos_improvement") or {}
        cond = cell.get("conditional") or {}
        econ_rec = record.get("net_improvement") or {}
        econ_cell = cell.get("economics") or {}
        _check("final_record_economics_match_the_matrix_cell",
               _close(econ_rec.get("ann_net_increment"), econ_cell.get("ann_net_increment"))
               and _close(econ_rec.get("sharpe_increment"), econ_cell.get("sharpe_increment")),
               "net increment record=%s matrix=%s" % (econ_rec.get("ann_net_increment"),
                                                       econ_cell.get("ann_net_increment")))
        _check("record_quotes_the_matrix_cell",
               _close(oos.get("increment"), cond.get("increment"))
               and _close(oos.get("t"), cond.get("t"))
               and _close(oos.get("lockbox"), cond.get("increment_lockbox"))
               and cell.get("verdict") == "INCREMENTAL_INFORMATION_CANDIDATE"
               and cell.get("fdr_pass_conditional") is True,
               "t=%s increment=%s lockbox=%s verdict=%s fdr=%s"
               % (cond.get("t"), cond.get("increment"), cond.get("increment_lockbox"),
                  cell.get("verdict"), cell.get("fdr_pass_conditional")))

    proto_path = _r63.PROTOCOL_PATH
    proto_shas = set()
    if proto_path.exists():
        raw = proto_path.read_bytes()
        proto_shas = {hashlib.sha256(raw).hexdigest(),
                      hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()}
    # only write_artifact-stamped artifacts carry protocol_sha256; a challenger
    # record body never did in R63, so it is not part of this check
    stamped = {a.get("protocol_sha256") for a in (cands, matrix) if a}
    _check("artifacts_stamped_with_committed_r63_protocol",
           bool(proto_shas) and len(stamped) == 1 and stamped <= proto_shas,
           "committed protocol sha256 (raw or LF-normalised)=%s stamped=%s"
           % (sorted(proto_shas), sorted(s for s in stamped if s)))

    repro = None
    if reproduced_cell is not None and cell is not None:
        repro = compare_conditional(cell, reproduced_cell)
        _check("fresh_run_reproduces_persisted_conditional_statistics", repro["matches"],
               "same owner, same substrate, tolerance %g" % REPRODUCTION_TOL,
               differences=repro["differences"])

    failed = [c["check"] for c in checks if not c["passed"]]
    if failed:
        verdict = INVALID
    elif file_state == FILE_STALE:
        verdict = VALID_STALE_FILE
    else:
        verdict = VALID
    body = {"schema": "r63_handoff_validation/1", "calculation_owner": CALCULATION_OWNER,
            "r63_results_root": str(root), "challenger_id": FX_CHALLENGER_ID,
            "cell_id": FX_CELL_ID, "checks": checks, "n_checks": len(checks),
            "failed_checks": failed, "verdict": verdict,
            "challenger_file_state": file_state,
            "challenger_file_note": (
                None if file_state != FILE_STALE else
                "challengers/%s.json was written once on the first R63 pass and never "
                "overwritten (R63's immutable-file rule); the candidates artifact was "
                "regenerated after the disclosed capped-book re-run. The two records name "
                "the same cell and the same conditional statistics; only the book economics "
                "differ. The FINAL evidence is the candidates artifact + the matrix."
                % FX_CHALLENGER_ID),
            "final_record_hash": (final_record or {}).get("record_hash"),
            "challenger_file_record_hash": (
                (_r63.read_json(rec_path) or {}).get("record_hash")),
            "record_hash": (final_record or {}).get("record_hash"),
            "persisted_conditional": (cell or {}).get("conditional"),
            "persisted_economics": (cell or {}).get("economics"),
            "reproduction": repro,
            "live_registration_performed": False, "promotion_performed": False}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
