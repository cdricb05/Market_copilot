r"""READ-ONLY probe: what does the CANONICAL market-session owner say about the
activation instant? Writes nothing, touches no store, opens no handle.

Run:  python _s25_epoch_fix\p_clock.py
"""
from __future__ import annotations

import sys
from importlib.util import spec_from_file_location
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent))


class _F:
    @classmethod
    def find_spec(cls, fullname, path=None, target=None):
        if fullname != "paper_trader":
            return None
        return spec_from_file_location(
            fullname, str(ROOT / "__init__.py"),
            submodule_search_locations=[str(ROOT)])


sys.meta_path.insert(0, _F)

import datetime as dt  # noqa: E402

from paper_trader.engine import exchange_calendar as XC  # noqa: E402
from paper_trader.engine import market_session as MS  # noqa: E402

ACTIVATED_AT = "2026-09-16T20:15:01.093648+00:00"
now = dt.datetime.fromisoformat(ACTIVATED_AT)

print("WORKTREE           =", ROOT)
print("CALENDAR_OWNER     = paper_trader.engine.exchange_calendar", XC.CALENDAR_ID)
print("SESSION_OWNER      = paper_trader.engine.market_session")
print("REGULAR_CLOSE_ET   =", MS.REGULAR_CLOSE_ET)
print("DEFAULT_CUTOFF_ET  =", MS.DEFAULT_CLOSE_CUTOFF_ET)
print("ACTIVATED_AT_UTC   =", ACTIVATED_AT)
print("ACTIVATED_AT_ET    =", MS.to_eastern(now).isoformat())
print("ACTIVATION_WEEKDAY =", MS.to_eastern(now).strftime("%A"))

ns = XC.non_sessions_between("2026-08-01", "2026-10-01")
print("NON_SESSIONS_AUG_OCT =", sorted(ns))
print("IS_NON_SESSION_0916  =", XC.is_non_session("2026-09-16"))
print("IS_NON_SESSION_0915  =", XC.is_non_session("2026-09-15"))

for label, cutoff in (("REGULAR_CLOSE_ET(16:00)", MS.REGULAR_CLOSE_ET),
                      ("DEFAULT_CUTOFF_ET(17:30)", MS.DEFAULT_CLOSE_CUTOFF_ET)):
    es = MS.resolve_expected_session(now, close_cutoff_et=cutoff, non_sessions=ns)
    print("  %-26s -> market_date=%s cutoff_passed=%s within_day=%s"
          % (label, es.market_date_iso, es.cutoff_passed, es.within_trading_day))

# The over-block control: BEFORE the close on the same session.
before = dt.datetime(2026, 9, 16, 11, 0, tzinfo=dt.timezone.utc)   # 07:00 ET
es = MS.resolve_expected_session(before, close_cutoff_et=MS.REGULAR_CLOSE_ET,
                                 non_sessions=ns)
print("BEFORE_CLOSE_0916_0700ET -> market_date=%s (must be 2026-09-15)"
      % es.market_date_iso)

print("NEXT_TRADING_DAY_AFTER_0916 =",
      MS.next_trading_day(dt.date(2026, 9, 16), ns).isoformat())
print("TODAY_ET =", MS.to_eastern(dt.datetime.now(dt.timezone.utc)).isoformat())
