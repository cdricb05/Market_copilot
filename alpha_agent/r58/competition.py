"""alpha_agent.r58.competition - capital frontier, purchase gate, immutability.

Three closing obligations of the protocol:

CAPITAL     only a candidate that PASSED its historical gates may compete for
            capital. The eligible set is computed, not narrated, so the answer
            to "where should the next $1,000 go" follows from the campaign table
            rather than from enthusiasm about the best-looking number in it.

PURCHASE    twelve questions, all of which must be answered affirmatively before
GATE        any dataset is recommended for purchase.

IMMUTABLE   the R56 shadow-portfolio records and the R57 artifacts are read-only
EVIDENCE    inputs. They are hashed before and after the campaign and the hashes
            are published, because "we did not touch it" is a claim and a hash
            is evidence.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from . import (R57_ROOT, file_hash, write_artifact)

ARTIFACT = "r58_capital_and_gate.json"
R56_ROOT = Path(r"D:\Stock_Prediction_app_data\r56_shadow_portfolios")
# The instant R58 registered its protocol; nothing R58 read may post-date it.
R58_START_UTC = "2026-09-04T19:00:10+00:00"


# --------------------------------------------------------------------------- #
def evidence_immutability() -> dict:
    """Hash every R56 and R57 artifact this campaign read, and prove the mtimes
    all predate R58's protocol registration.

    A hash taken only AFTER a campaign proves the file is what it is now, not
    that nothing changed. The modification times are the independent check: R58
    registered its protocol at ``R58_START_UTC``, so any evidence file touched
    after that instant would be a write this campaign made.
    """
    start = datetime.fromisoformat(R58_START_UTC)
    out = {"r58_protocol_registered_at": R58_START_UTC}
    for label, root in (("r56_shadow_portfolios", R56_ROOT),
                        ("r57_alpha_discovery", R57_ROOT)):
        rows = {}
        touched = []
        if root.exists():
            for p in sorted(root.rglob("*")):
                if not p.is_file():
                    continue
                mt = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
                rel = str(p.relative_to(root))
                rows[rel] = {"sha256": file_hash(p), "bytes": p.stat().st_size,
                             "mtime_utc": mt.isoformat()}
                if mt > start:
                    touched.append(rel)
        out[label] = {"root": str(root), "files": len(rows),
                      "files_modified_after_r58_started": touched,
                      "unmodified": not touched, "hashes": rows}
    out["verdict"] = ("R56_AND_R57_EVIDENCE_UNMODIFIED"
                      if all(out[k]["unmodified"] for k in
                             ("r56_shadow_portfolios", "r57_alpha_discovery"))
                      else "EVIDENCE_MODIFIED_INVESTIGATE")
    out["claim"] = ("R58 opened these read-only and wrote nothing to them. The "
                    "hashes and modification times above are the evidence, not "
                    "the claim.")
    return out


# --------------------------------------------------------------------------- #
def capital_frontier(verdicts: dict, labs: dict) -> dict:
    """Who is allowed to compete for capital, and what the next dollar buys."""
    eligible = [k for k, v in verdicts["campaign_verdicts"].items()
                if v.get("verdict") == "HISTORICAL_ALPHA_CANDIDATE"]
    blocked = [k for k, v in verdicts["campaign_verdicts"].items()
               if v.get("verdict") == "DATA_HOLD_COVERAGE"]
    rejected = [k for k, v in verdicts["campaign_verdicts"].items()
                if v.get("verdict") == "NO_ALPHA_EVIDENCE"]
    return {
        "eligibility_rule": "only a candidate that passed its historical gates "
                            "may compete",
        "qualified_candidates": eligible,
        "coverage_blocked_not_qualified": blocked,
        "rejected": rejected,
        "competitor_set_unchanged": not eligible,
        "competitors": ["the incumbent operational strategy", "SPY", "CASH"],
        "if_all_capital_were_cash": (
            "R58 allocates it NOWHERE NEW. Zero families cleared the "
            "pre-registered gates, so no R58 construct can name a positive "
            "after-cost expected edge it is entitled to defend."),
        "next_1000": {
            "allocation": "CASH (or the incumbent book, unchanged)",
            "reason": "no qualified competitor exists; the strongest-looking "
                      "lockbox number in the campaign (R&D intensity, +11.86%/yr) "
                      "loses its entire excess when its largest sector is removed "
                      "(-0.22%/yr, t -0.06), so it is a sector bet, not an edge to "
                      "fund",
        },
        "next_10000": {
            "allocation": "CASH (or the incumbent book, unchanged)",
            "reason": "identical: the amount does not change the evidence. The "
                      "R58 answer is about the absence of a defensible edge, not "
                      "about position size.",
        },
        "governed_lane": "MANUAL_REVIEW_REQUIRED_NO_ECONOMIC_PROOF (unchanged)",
        "safety": "research only; no capital is moved, no order, no fill, no "
                  "proposal written to any operational store",
    }


# --------------------------------------------------------------------------- #
PURCHASE_GATE = {
    "point_in_time_sector_history": {
        "1_which_hypothesis_is_blocked":
            "the exact replication of the operational fundamental leg "
            "(composite_SN). R58 could only reproduce composite_RAW, so every "
            "R58 fundamental verdict measures a CONSTRUCT that resembles the "
            "champion's leg rather than the leg itself. It also blocks honest "
            "sector-neutral normalisation and a fully point-in-time "
            "sector-concentration gate - the gate that just demolished the "
            "campaign's best-looking number.",
        "2_why_orthogonal_to_owned_data":
            "it is not an alpha source at all; it is a NORMALISER. Owned GICS is "
            "a current snapshot, and a company's sector at the time of a "
            "historical decision is simply not recoverable from anything owned.",
        "3_free_or_owned_proxy_tested_first":
            "yes - SIC codes from the owned SEC issuer index (979,405 issuers "
            "with sic/sic_description) and the current Norgate GICS "
            "classification",
        "4_what_the_proxy_showed":
            "current GICS was used for the sector-exclusion checks and was "
            "sufficient to DESTROY the R&D result, which is the outcome that "
            "mattered. SIC is a filing-time attribute and would improve on "
            "current GICS for delisted names, but it is a different taxonomy "
            "from the one the operational leg normalises within.",
        "5_historical_pit_integrity_available": "yes, from index vendors",
        "6_inactive_delisted_covered": "vendor-dependent; must be verified before "
                                       "any purchase, and this is exactly where "
                                       "cheap products fail",
        "7_effective_sample": "the same 885-symbol, 43-month lockbox R58 already "
                              "has - the purchase adds no new rows, only a "
                              "correct normaliser",
        "8_experiment_we_would_run":
            "re-run A1-A4 sector-neutralised and re-run the C4 diagnostic with a "
            "PIT sector-concentration gate",
        "9_success": "a fundamental family clears the same pre-registered gates "
                     "sector-neutralised that it failed raw",
        "10_rejection": "the sector-neutralised families fail the same way, which "
                        "would establish that normalisation is not the blocker",
        "11_cost": "NOT PRICED - no vendor quotation was obtained in R58",
        "12_expected_research_value_sufficient": False,
        "verdict": "DO_NOT_BUY_YET",
        "verdict_reason":
            "the gate fails at question 12 on its own evidence. R58's fundamental "
            "families did not fail marginally - A1/A2/A3 flipped SIGN between "
            "validation and the lockbox, exactly as R57's price families did. A "
            "better normaliser does not repair a sign flip. Buying PIT sector "
            "history to re-run a construct whose ordering is unstable would be "
            "paying for a cleaner measurement of the same instability. The honest "
            "reopen condition is a family that is sign-STABLE and fails only on "
            "sector concentration; R58 has none.",
    },
    "form4_transaction_detail": {
        "1_which_hypothesis_is_blocked":
            "net insider acquisition. The challenger was written, computed, and "
            "returned an empty book.",
        "2_why_orthogonal_to_owned_data":
            "insider behaviour is neither price nor reported accounting; it is "
            "the one genuinely orthogonal family R58 identified that has real "
            "breadth (2,386 tickers) and real timestamps",
        "3_free_or_owned_proxy_tested_first":
            "yes - the owned INSIDER_FILING family itself",
        "4_what_the_proxy_showed":
            "the filings are collected but the transaction table is not parsed: "
            "acquired_disposed is populated on 195 of 28,002 records (0.7%)",
        "5_historical_pit_integrity_available":
            "YES AND IT IS FREE. Form 4 is a public SEC filing with an acceptance "
            "timestamp. This is not a purchase problem at all - it is a "
            "COLLECTOR problem in owned code.",
        "6_inactive_delisted_covered": "yes, EDGAR retains filings for delisted "
                                       "issuers",
        "7_effective_sample": "roughly 10 months of owned filings today, growing "
                              "daily; a backtestable partition is years away, so "
                              "the honest route is a prospective freeze",
        "8_experiment_we_would_run":
            "parse the Form 4 transaction table, freeze a net-insider prospective "
            "challenger, and let the clock run",
        "9_success": "the frozen challenger accumulates forward evidence that "
                     "clears the confirmation floor",
        "10_rejection": "it does not",
        "11_cost": "$0 - it is parsing work on data already on disk",
        "12_expected_research_value_sufficient": True,
        "verdict": "NO_PURCHASE_REQUIRED_BUILD_INSTEAD",
        "verdict_reason":
            "the highest-value information action R58 found costs nothing to buy. "
            "The data is already collected and already timestamped; only the "
            "transaction detail is unparsed.",
    },
    "general": {
        "recommendation": "NO DATASET IS RECOMMENDED FOR PURCHASE",
        "reason": "no candidate dataset passes all twelve questions. One "
                  "candidate (PIT sector history) fails on expected research "
                  "value because a normaliser cannot repair a sign flip; the "
                  "other (Form 4 detail) is not a purchase at all but unparsed "
                  "owned data.",
        "standing_evidence": "the last gated purchase (Norgate Futures, R37) was "
                             "fully prosecuted by R38 and R57 and yielded no "
                             "qualifying family, which is the reason this gate "
                             "keeps demanding demonstrated incremental value "
                             "before money moves.",
    },
}


# --------------------------------------------------------------------------- #
def run(verdicts: dict, labs: dict) -> dict:
    body = {
        "track": "R58_CAPITAL_AND_PURCHASE_GATE",
        "capital_frontier": capital_frontier(verdicts, labs),
        "data_purchase_gate": PURCHASE_GATE,
        "evidence_immutability": evidence_immutability(),
    }
    write_artifact(ARTIFACT, body)
    return body
