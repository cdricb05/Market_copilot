r"""Release 62.3.1 - the LIVE snapshot contract for the frozen reversed-skew
challenger, and the proof that preparing it changed nothing about the rule.

THE SITUATION THESE TESTS GUARD
    ``REVERSED_SPY_PUT_CALL_SKEW_H5`` is historically confirmed and cannot begin
    prospective evidence, because its declared information cutoff (15:45 ET) and
    its entry mark (16:00 ET) both fall INSIDE the session it decides on, and the
    historical API publishes a session only after that session ends. The gap is
    an entitlement, not a defect, so the repository must hold a CONTRACT for the
    missing feed rather than an adapter pretending to be one.

    The danger of writing such a contract is that it quietly becomes a second
    definition of the strategy - a second band, a second snapshot minute, a
    second implied volatility - and the confirmation would then belong to the
    re-typing rather than to the hypothesis. Every test below exists to keep the
    contract DERIVED from the frozen owners instead of restating them.

    The one substantive question the contract had to answer is the underlying
    mark: the surface builder refuses a date with no underlying close, and the
    frozen close is the 16:00 mark, fifteen minutes after the cutoff. Test 09
    settles it by measurement rather than by argument.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from paper_trader.alpha_agent.alpha_recovery import live_snapshot_contract as LSC
from paper_trader.alpha_agent.alpha_recovery import options_acquisition as OA
from paper_trader.alpha_agent.alpha_recovery import options_surface as OS

SRC = Path(LSC.__file__).read_text(encoding="utf-8")
CODE = "\n".join(ln for ln in SRC.splitlines() if not ln.strip().startswith("#"))
IMPORTS = "\n".join(ln for ln in SRC.splitlines()
                    if ln.lstrip().startswith(("import ", "from ")))

SESSION = "2026-09-14"


# --------------------------------------------------------------- the feed
def test_01_the_contract_never_respells_the_frozen_feed():
    """A second spelling of the dataset, schema or snapshot minute could drift
    away from the surface the sign was discovered on. They must DELEGATE."""
    assert LSC.REQUIRED_DATASET == OA.DATASET
    assert LSC.MINIMUM_SCHEMA == OA.SCHEMA
    assert LSC.SNAPSHOT_ET == OA.SNAPSHOT_ET
    assert "REQUIRED_DATASET = OA.DATASET" in CODE
    assert "MINIMUM_SCHEMA = OA.SCHEMA" in CODE
    assert "SNAPSHOT_ET = OA.SNAPSHOT_ET" in CODE
    # and the band is never re-derived here
    assert "MONEYNESS_BAND =" not in CODE
    assert "STRIKE_SPACING =" not in CODE
    assert "LOOKBACK_DAYS =" not in CODE


def test_02_the_required_fields_are_exactly_what_the_surface_builder_reads():
    """If the feed delivers a field the builder does not read, it is surplus; if
    it omits one the builder reads, the snapshot cannot be normalised at all."""
    builder = Path(OA.__file__).read_text(encoding="utf-8")
    start = builder.index("def build_surface(")
    body = builder[start:builder.index("def information_case(")]
    for field in LSC.REQUIRED_FIELDS:
        assert '"%s"' % field in body, "%s is not read by build_surface" % field
    # the price fields the inversion actually consumes
    assert "bid_px_00" in LSC.REQUIRED_FIELDS and "ask_px_00" in LSC.REQUIRED_FIELDS


def test_11_the_minimum_schema_is_the_frozen_one_and_richer_feeds_are_refused():
    ch = LSC.SCHEMA_CHOICE
    assert ch["minimum_sufficient"]["schema"] == OA.SCHEMA
    assert ch["acceptable_superset"]["schema"] == "cbbo-1s"
    assert "cmbp-1" in ch["rejected"]
    # the rejection is quantitative, not stylistic
    assert (ch["rejected"]["cmbp-1"]["bytes_one_session_116_symbols"]
            > ch["minimum_sufficient"]["bytes_one_session_116_symbols"] * 100)
    assert (ch["acceptable_superset"]["bytes_one_session_116_symbols"]
            > ch["minimum_sufficient"]["bytes_one_session_116_symbols"] * 10)


# ------------------------------------------------------------- the symbols
def test_03_the_near_leg_is_the_first_expiry_far_enough_out_to_be_one():
    """A live capture that bought only the front monthly would produce a NaN
    feature on exactly the sessions closest to an expiry - which is how nine of
    fifteen catch-up sessions arrived empty before the October band was added."""
    nf = LSC.near_and_far(SESSION)
    assert nf["expiries"] == ["2026-09-18", "2026-10-16", "2026-11-20"]
    assert nf["near"] == "2026-10-16", "the September monthly is too close to be a near leg"
    assert nf["far"] == "2026-11-20"
    d = date.fromisoformat(SESSION)
    assert (date.fromisoformat("2026-09-18") - d).days / 365.25 < OS.MIN_T_YEARS_NEAR
    assert (date.fromisoformat(nf["near"]) - d).days / 365.25 >= OS.MIN_T_YEARS_NEAR


def test_03b_an_expiry_beyond_the_frozen_lookback_is_not_live_on_the_session():
    """The historical plan buys each expiry over the LOOKBACK_DAYS before it, so
    a live capture that subscribed further out would hold an expiry the surface
    never carried on that date."""
    exps = LSC.expiries_for(SESSION)
    d = date.fromisoformat(SESSION)
    for e in exps:
        assert 0 <= (e - d).days <= OA.LOOKBACK_DAYS
    assert date.fromisoformat("2026-12-18") not in exps


def test_04_symbols_round_trip_through_the_frozen_osi_spelling():
    """The venue refused two other spellings; the contract must not invent a
    third."""
    level = 660.0
    book = LSC.symbols_for(SESSION, level)
    assert set(book) == {e.isoformat() for e in LSC.expiries_for(SESSION)}
    for exp, syms in book.items():
        assert len(syms) == len(OA.band_for(level)) * 2      # a call and a put
        strikes = set()
        for s in syms:
            m = OA.parse_osi(s)
            assert m is not None, "%r is not an OSI symbol" % s
            assert m["expiration"].isoformat() == exp
            assert m["type"] in ("call", "put")
            strikes.add(m["strike"])
        assert min(strikes) <= level <= max(strikes), "the band must bracket the money"


# ------------------------------------------------------- what it may not be
def test_05_the_contract_holds_no_client_no_socket_and_no_credential():
    """A contract that could open a connection would eventually be pointed at
    historical files, and a same-session decision formed from data published
    after the session closed is the exact dishonesty being prevented."""
    for tok in ("import socket", "import urllib", "import ssl", "urlopen(",
                "socket.", "api_key(", "DA.Client", "databento_acquisition"):
        assert tok not in CODE, "the contract must not be able to fetch: %s" % tok
    assert "hashlib" not in IMPORTS


def test_06_the_contract_cannot_evaluate_the_rule_or_touch_capital():
    """It supplies information. Every decision stays with its existing owner."""
    for tok in ("np.sign(", "import numpy", "_implied_vol(", "OS._z(",
                "def freeze", "weights", "position ="):
        assert tok not in CODE, "the feed owner must not reach %s" % tok


def test_07_a_missed_session_is_declared_permanently_missed():
    p = LSC.PROHIBITIONS
    assert "must_not_backfill_a_missed_session" in p
    assert "for ever" in p["must_not_backfill_a_missed_session"]
    assert LSC.blocking_state()["a_missed_session_stays_missed"] is True
    assert "append only" in p["must_not_overwrite_history"].lower()


def test_10_the_remaining_work_never_reopens_the_frozen_rule():
    """Every operator prohibition is written down, and none of the remaining
    steps touches the sign, the horizon or the lookback."""
    for required in ("must_not_change_the_signal", "must_not_choose_a_position",
                     "must_not_promote_anything", "must_not_allocate_capital",
                     "must_not_create_an_order_or_a_fill",
                     "must_not_backfill_a_missed_session"):
        assert required in LSC.PROHIBITIONS
    steps = " ".join(LSC.REMAINING_IMPLEMENTATION).lower()
    for reopened in ("sign", "horizon", "z-score", "lookback", "tune", "alternate"):
        assert reopened not in steps, "step list reopens %r" % reopened
    assert len(LSC.REMAINING_IMPLEMENTATION) <= 6


# --------------------------------------------------------- the entitlement
def test_08_the_entitlement_is_recorded_as_absent_not_as_a_bad_credential():
    """The distinction matters: a rejected key is a configuration problem
    somebody could fix here, and a missing license is a purchase decision that
    only the operator can make."""
    probe = LSC.ENTITLEMENT_PROBE
    assert probe["billable"] is False
    assert probe["credential_was_rejected"] is False
    for ds in ("OPRA.PILLAR", "EQUS.SUMMARY"):
        g = probe["gateways"][ds]
        assert g["entitled"] is False
        assert "live data license is required" in g["response"]
        assert "success=0" in g["response"]
    st = LSC.blocking_state()
    assert st["state"] == "BLOCKED_ON_LIVE_DATA_ENTITLEMENT"
    assert st["entitled"] is False


# ------------------------------------------------ the one substantive claim
def _surface(tmp_path, name, mark):
    """A minimal surface in the exact shape ``options_surface.features`` reads."""
    rows = []
    for i, d in enumerate(("2026-01-05", "2026-01-06", "2026-01-07")):
        for k in range(90, 111, 5):
            for right in ("call", "put"):
                rows.append({
                    "date": d, "expiration": "2026-04-17", "type": right,
                    "strike": float(k),
                    # a smile that differs per date so `skew` is not constant
                    "iv": 0.20 + 0.001 * i + (0.02 if right == "put" else 0.0)
                          + 0.0005 * (100 - k),
                    "moneyness": k / 100.0,
                    "T_years": 0.28,
                    "underlying_close": mark,
                })
    p = tmp_path / name
    pd.DataFrame(rows).to_csv(p, index=False)
    return p


def test_09_the_frozen_feature_is_invariant_to_the_underlying_mark(tmp_path):
    """THE claim the live contract rests on.

    A 15:45 decision cannot see the 16:00 close, so a same-session row must
    carry a 15:45 underlying mark instead. That substitution is legitimate ONLY
    if it cannot move the frozen feature. It cannot: the feature reads implied
    volatility and FORWARD moneyness, and the forward comes from put-call parity
    inside the snapshot itself.
    """
    base = OS.features(rebuild=True, surface=_surface(tmp_path, "a.csv", 100.0))
    pert = OS.features(rebuild=True, surface=_surface(tmp_path, "b.csv", 103.0))

    assert np.isfinite(base["skew"]).any(), "the fixture must produce a real skew"
    for col in ("skew", "atm_iv_near", "atm_iv_far", "term_slope"):
        assert np.array_equal(base[col].to_numpy(), pert[col].to_numpy(),
                              equal_nan=True), "%s moved with the underlying mark" % col

    # and the column the rule does NOT read is the only one that moves
    assert not np.array_equal(base["underlying_close"].to_numpy(),
                              pert["underlying_close"].to_numpy())

    # the contract states this as the reason, so the claim and the code agree
    why = LSC.UNDERLYING_MARK["why_it_cannot_change_the_signal"]
    assert "rv21" in why and "vrp" in why
    assert "16:00 close" in LSC.UNDERLYING_MARK["forbidden"]
