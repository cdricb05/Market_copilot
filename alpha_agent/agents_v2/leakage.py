r"""alpha_agent.agents_v2.leakage - a leakage check that is VERIFIED, not asserted.

WHAT WENT WRONG
---------------
R67 published ``fs_r67_terms_of_trade_residual`` with ``leakage_check = "PASS"``
and the governed pipeline accepted it. The feature set was leaky. Each feature
carried ``"lag": "1 session"`` and, on the next line of the same JSON object, a
formula that read session ``t`` for every return term:

    residual_6A_t = ret_6A_t - beta_6A_t * mean(ret_i_t for i in [GC, HG, SI])

Only ``beta`` was lagged. The effective lag was 0 against a layer whose own
declaration is ``SIGNAL_LAG_SESSIONS = 1`` - "scores read data through index
t-1, because run_book enters at the settlement of t". The label and the formula
contradicted each other inside one object, and the label was the wrong one.

``publish_features`` could not catch it, and the reason is worth stating exactly:
it enforced the SHAPE of the claim - the string equals ``"PASS"``, every feature
carries ``name``, ``lag`` and ``source`` - and had no way to evaluate the TRUTH
of the claim against the timing rule of the data the features are built from. A
check the author performs on their own work and reports as a string is not a
check; it is a claim about a check.

WHAT THIS MODULE DOES INSTEAD
-----------------------------
It reads the FORMULA and works out, arithmetically, the newest session index the
feature touches. It compares that to the lag the DATASET's own certification
declares. Three things must agree, and any disagreement is a refusal:

    the dataset's declared timing rule   (how stale the newest usable input is)
    the feature's declared ``lag``       (what the author says it reads)
    the feature's ``formula``            (what it actually reads)

So the R67 feature set is refused on its own text, by arithmetic, with no
judgement and nothing for an author to assert their way past.

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
It does not try to understand the economics of a feature, and it is not a
substitute for the skeptic's PIT attack. It answers exactly one question - does
this formula read data the execution rule forbids - and it answers it
conservatively: a formula it cannot parse is REFUSED rather than passed, because
"I could not check this" and "this is fine" are different answers and only one
of them is safe to default to.

RESEARCH ONLY. It reads text and returns a verdict. It writes nothing.
"""
from __future__ import annotations

import re
from typing import Any, Optional

CALCULATION_OWNER = "alpha_agent.agents_v2.leakage"

#: Verdicts.
PASS = "PASS"
#: A feature that declares no formula. NOT the same as a violation: nothing about
#: its timing can be checked either way, and saying so is the honest answer.
NOT_MACHINE_VERIFIABLE = "NOT_MACHINE_VERIFIABLE_NO_FORMULA_DECLARED"
REFUSED_LAG_VIOLATION = "FEATURE_READS_DATA_THE_EXECUTION_RULE_FORBIDS"
REFUSED_LABEL_DISAGREES = "DECLARED_LAG_DISAGREES_WITH_THE_FORMULA"
REFUSED_UNPARSEABLE = "FORMULA_CARRIES_NO_READABLE_TIME_INDEX"
REFUSED_NO_TIMING_RULE = "DATASET_DECLARES_NO_TIMING_RULE"
REFUSED_NO_FORMULA = "FEATURE_DECLARES_NO_FORMULA"
VERDICTS = (PASS, NOT_MACHINE_VERIFIABLE, REFUSED_LAG_VIOLATION,
            REFUSED_LABEL_DISAGREES, REFUSED_UNPARSEABLE,
            REFUSED_NO_TIMING_RULE, REFUSED_NO_FORMULA)

#: WHY A MISSING FORMULA IS RECORDED RATHER THAN REFUSED, AND WHAT WOULD CHANGE
#: THAT.
#:
#: The R67 defect was a feature WITH a formula whose formula contradicted its own
#: label. That is refused here, hard, and cannot be published. A feature with NO
#: formula is a different and older gap: ``formula`` has never been a required
#: lineage field, so refusing every such feature would reject essentially every
#: feature set the estate has ever published - a change far larger than closing
#: the defect, and one that would block research rather than protect it.
#:
#: So an unverifiable feature set publishes, and publishes WITH THAT FACT
#: ATTACHED: ``verified`` is False, the verdict is NOT_MACHINE_VERIFIABLE, and
#: ``preregister`` copies the flag into the frozen spec of every experiment built
#: on it. The gap is now measurable instead of invisible, which is the
#: precondition for closing it. Flipping this constant to True makes a formula
#: mandatory; do that once the estate's published feature sets carry them.
REQUIRE_MACHINE_VERIFIABLE_FORMULA = False

#: The STRICTEST default. A dataset whose certification declares no timing rule
#: is held to a one-session lag, which is the rule every layer in this estate
#: declares and the only one a decision entered at a session's own mark can
#: honour. Defaulting to 0 would make an undeclared dataset the easiest place to
#: leak, which is precisely backwards.
DEFAULT_SIGNAL_LAG_SESSIONS = 1

#: Column-name fragments whose publication lag is LONGER than the signal lag,
#: with the key in a dataset's timing rule that declares how much longer. Open
#: interest is the estate's measured case: the exchange publishes session s on
#: s+1, so with a one-session signal lag the newest usable index is t-2
#: (``data_r38.OI_LATEST_USABLE_OFFSET``). A feature touching one of these is
#: held to the longer rule.
LATE_PUBLISHED_INPUTS = {
    "open_interest": "oi_latest_usable_offset",
    "oi_aggregate": "oi_latest_usable_offset",
    "_oi": "oi_latest_usable_offset",
}

#: Every way this estate's feature formulas have written a time index.
#:   ret_6A_t        ret_t         x[t]        close_t
#:   ret_6A_{t-1}    ret_{t-1}     x[t-1]      close_t_minus_1
#:   beta_6A_t-21
_INDEX_PATTERNS = (
    re.compile(r"\{\s*t\s*-\s*(\d+)\s*\}"),        # {t-1}
    re.compile(r"\[\s*t\s*-\s*(\d+)\s*\]"),        # [t-1]
    re.compile(r"_t\s*-\s*(\d+)\b"),               # _t-21
    re.compile(r"\bt\s*-\s*(\d+)\b"),              # t - 1
    re.compile(r"_t_minus_(\d+)\b"),               # _t_minus_1
)
#: A bare ``t`` - lag 0. Written so it cannot also match the ``t-1`` forms
#: above, which are stripped before this one is applied.
_BARE_T = re.compile(r"(?:\{\s*t\s*\}|\[\s*t\s*\]|_t\b|\bt\b)")


def _strip_lagged(formula: str) -> str:
    """Remove every EXPLICITLY LAGGED index, so what remains is lag-0 reads."""
    out = formula
    for pat in _INDEX_PATTERNS:
        out = pat.sub(" LAGGED ", out)
    return out


def effective_lag(formula: Any) -> dict:
    """The NEWEST session index a formula reads, as a lag in sessions.

    The minimum over every time-indexed read, because a feature is only as
    point-in-time as its freshest input. R67's residual lagged ``beta`` and not
    ``ret``, and its minimum was 0 - which is the whole answer.
    """
    text = str(formula or "").strip()
    if not text:
        return {"resolved": False, "reason": REFUSED_NO_FORMULA,
                "effective_lag": None, "reads": []}
    lags = []
    for pat in _INDEX_PATTERNS:
        for m in pat.finditer(text):
            try:
                lags.append(int(m.group(1)))
            except (TypeError, ValueError):
                continue
    if _BARE_T.search(_strip_lagged(text)):
        lags.append(0)
    if not lags:
        return {"resolved": False, "reason": REFUSED_UNPARSEABLE,
                "effective_lag": None, "reads": [],
                "detail": ("no time index could be read from %r. A formula this "
                           "check cannot parse is refused rather than passed: "
                           "'I could not check this' is not 'this is fine'."
                           % text[:160])}
    return {"resolved": True, "effective_lag": min(lags),
            "reads": sorted(set(lags)), "reason": None}


def declared_lag(value: Any) -> Optional[int]:
    """The integer inside a declared lag label such as ``"1 session"``."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    m = re.search(r"(-?\d+)", str(value or ""))
    return int(m.group(1)) if m else None


def required_lag(feature: dict, timing_rule: Optional[dict]) -> dict:
    """How stale this feature's newest input must be, per the DATASET's rule."""
    rule = dict(timing_rule or {})
    base = rule.get("signal_lag_sessions")
    try:
        base = int(base) if base is not None else DEFAULT_SIGNAL_LAG_SESSIONS
    except (TypeError, ValueError):
        base = DEFAULT_SIGNAL_LAG_SESSIONS
    hay = "%s %s %s" % (feature.get("name"), feature.get("formula"),
                        feature.get("source"))
    hay = hay.lower()
    late = None
    for token, key in LATE_PUBLISHED_INPUTS.items():
        if token in hay:
            declared = rule.get(key)
            try:
                declared = int(declared) if declared is not None else base + 1
            except (TypeError, ValueError):
                declared = base + 1
            if late is None or declared > late:
                late = declared
                late_token = token
    if late is not None and late > base:
        return {"required_lag": late, "because": (
            "the formula touches %r, whose publication lags the session it "
            "describes; the dataset's timing rule puts the newest usable index "
            "at t-%d" % (late_token, late))}
    return {"required_lag": base, "because": (
        "the dataset declares signal_lag_sessions=%d: a score for a decision at "
        "index t may read data through index t-%d only" % (base, base))}


def check_feature(feature: dict, timing_rule: Optional[dict]) -> dict:
    """Verify ONE feature's formula against the dataset's declared timing rule."""
    f = dict(feature or {})
    out = {"name": f.get("name"), "declared_lag": f.get("lag"),
           "formula": f.get("formula"), "source": f.get("source"),
           "calculation_owner": CALCULATION_OWNER}
    req = required_lag(f, timing_rule)
    out.update(req)
    eff = effective_lag(f.get("formula"))
    out["effective_lag"] = eff.get("effective_lag")
    out["time_indices_read"] = eff.get("reads")
    if eff.get("reason") == REFUSED_NO_FORMULA:
        # Unverifiable, and said so. See REQUIRE_MACHINE_VERIFIABLE_FORMULA.
        return {**out, "verdict": (REFUSED_NO_FORMULA
                                   if REQUIRE_MACHINE_VERIFIABLE_FORMULA
                                   else NOT_MACHINE_VERIFIABLE),
                "passes": not REQUIRE_MACHINE_VERIFIABLE_FORMULA,
                "machine_verifiable": False,
                "detail": ("the feature declares no formula, so nothing about "
                           "its timing can be verified. This is recorded, not "
                           "taken on trust: the published feature set carries "
                           "verified=False and every experiment built on it "
                           "carries that flag in its frozen spec.")}
    if not eff.get("resolved"):
        return {**out, "verdict": eff["reason"], "passes": False,
                "machine_verifiable": False,
                "detail": eff.get("detail")}
    if eff["effective_lag"] < req["required_lag"]:
        return {**out, "verdict": REFUSED_LAG_VIOLATION, "passes": False,
                "detail": ("the formula reads index t-%d and %s. The feature is "
                           "LEAKY as written, whatever its leakage_check says."
                           % (eff["effective_lag"], req["because"]))}
    dec = declared_lag(f.get("lag"))
    if dec is not None and dec != eff["effective_lag"]:
        return {**out, "verdict": REFUSED_LABEL_DISAGREES, "passes": False,
                "detail": ("the feature declares a lag of %d session(s) and its "
                           "own formula reads index t-%d. A label and a formula "
                           "that disagree inside one object are not a lineage "
                           "record; one of them is wrong and this check will not "
                           "guess which." % (dec, eff["effective_lag"]))}
    return {**out, "verdict": PASS, "passes": True,
            "machine_verifiable": True,
            "detail": ("reads index t-%d against a required lag of t-%d"
                       % (eff["effective_lag"], req["required_lag"]))}


def check_feature_set(*, features: list, timing_rule: Optional[dict],
                      dataset_id: str = "", asserted: str = "") -> dict:
    """Verify a whole feature set. Any single failure fails the set.

    ``asserted`` is the author's own ``leakage_check`` string. It is reported so
    a false PASS is visible beside the verdict that contradicts it, and it is
    never used to decide anything.
    """
    rows = [check_feature(f, timing_rule) for f in (features or [])]
    failures = [r for r in rows if not r["passes"]]
    unverifiable = [r for r in rows if r.get("machine_verifiable") is False]
    blocked = bool(failures)
    # VERIFIED means every feature was actually checked and passed. A set with
    # an unverifiable feature is publishable and is NOT verified, and the two
    # words are kept apart so nothing downstream can read one as the other.
    ok = bool(rows) and not failures and not unverifiable
    return {
        "calculation_owner": CALCULATION_OWNER,
        "dataset_id": dataset_id,
        "timing_rule": dict(timing_rule or {}),
        "timing_rule_declared": bool(timing_rule),
        "verdict_vocabulary": list(VERDICTS),
        "verdict": (PASS if ok else (failures[0]["verdict"] if failures
                                     else NOT_MACHINE_VERIFIABLE)),
        "verified": ok,
        "publishable": bool(rows) and not blocked,
        "n_features": len(rows),
        "n_failed": len(failures),
        "n_not_machine_verifiable": len(unverifiable),
        "not_machine_verifiable": [r["name"] for r in unverifiable],
        "failures": failures,
        "features": rows,
        "asserted_by_the_author": asserted,
        "author_assertion_contradicted": bool(
            not ok and str(asserted or "").upper() == PASS),
        "note": ("the author's leakage_check is reported and never consulted. "
                 "A check performed by the author on their own work and "
                 "reported as a string is a claim about a check, not a check."),
    }


__all__ = ["CALCULATION_OWNER", "PASS", "NOT_MACHINE_VERIFIABLE",
           "REQUIRE_MACHINE_VERIFIABLE_FORMULA", "REFUSED_LAG_VIOLATION",
           "REFUSED_LABEL_DISAGREES", "REFUSED_UNPARSEABLE",
           "REFUSED_NO_TIMING_RULE", "REFUSED_NO_FORMULA", "VERDICTS",
           "DEFAULT_SIGNAL_LAG_SESSIONS", "LATE_PUBLISHED_INPUTS",
           "effective_lag", "declared_lag", "required_lag", "check_feature",
           "check_feature_set"]
