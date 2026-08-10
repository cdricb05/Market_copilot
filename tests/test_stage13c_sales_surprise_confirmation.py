"""Stage 13C — frozen out-of-sample sales-surprise confirmation: contract
integrity tests.

Covers (pure functions; no D: drive, no network, no operational store):
  * the frozen contract constants cannot drift silently;
  * discovery/confirmation separation is HARD (dates AND provider ids);
  * admission refuses in-discovery / signal-null / ticker-less events;
  * identity resolution is survivorship-safe (delisted members retained,
    effective-dated tickers, unresolved counted — never patched);
  * execution is PIT (entry strictly after the report date) and an unmatured
    horizon is EXCLUDED — never substituted, never matured early;
  * the frozen statistic (monthly cohorts >= 25 events, quintile Q5-Q1,
    25 bps/side cost) is applied exactly;
  * the conservative disposition logic (no promotion, ever).
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO.parent))

from paper_trader.scripts import stage13c_sales_surprise_confirmation as s13c  # noqa: E402


def _payload(rid, date, ticker="AAA", signal=1.0, **kw):
    p = {"id": rid, "actual_reported_date": date,
         "sales_percent_diff": signal, "security": {"ticker": ticker}}
    p.update(kw)
    return p


class FrozenContractConstants(unittest.TestCase):
    """The constants ARE the contract; a change here is a tuning violation."""

    def test_frozen_values(self):
        self.assertEqual(s13c.SIGNAL_FIELD, "sales_percent_diff")
        self.assertEqual(s13c.HORIZONS, (5, 20, 63))
        self.assertEqual(s13c.PRIMARY_HORIZON, 63)
        self.assertEqual(s13c.N_QUANTILES, 5)
        self.assertEqual(s13c.COHORT_MIN_EVENTS, 25)
        self.assertEqual(s13c.COST_PER_SIDE, 0.0025)
        self.assertEqual(s13c.ROUND_TRIP_COST, 0.005)
        self.assertEqual(s13c.T_PRIMARY_BONFERRONI, 2.39)
        self.assertEqual(s13c.T_OVERLAP_HONEST, 2.0)
        self.assertEqual(s13c.DISCOVERY_REQUEST_START, "2024-12-01")
        self.assertEqual(s13c.CONFIRMATION_END, "2024-11-30")
        self.assertEqual(s13c.MIN_COHORTS, 24)
        self.assertEqual(s13c.MIN_EVENTS_TOTAL, 1000)
        self.assertEqual(s13c.HIT_RATE_MIN, 0.55)

    def test_no_operational_write_imports(self):
        src = Path(s13c.__file__).read_text(encoding="utf-8")
        for forbidden in ("paper_trading_desk", "operational_book",
                          "alpha_book", "alpha_target", "daily_close",
                          "reallocation_proposal"):
            self.assertNotIn(forbidden, src)


class AdmissionRule(unittest.TestCase):
    def test_refuses_discovery_window_and_defects(self):
        events = [
            _payload("a", "2024-11-29"),                      # admissible
            _payload("b", "2024-12-01"),                      # discovery start
            _payload("c", "2025-03-01"),                      # inside discovery
            _payload("d", "2024-10-01", signal=None),         # null signal
            _payload("e", "2024-10-01", ticker=""),           # no ticker
            {"id": "f", "sales_percent_diff": 1.0,
             "security": {"ticker": "AAA"}},                  # no date
        ]
        out = s13c.admissible_confirmation_events(events)
        self.assertEqual([e["id"] for e in out["events"]], ["a"])
        self.assertEqual(out["refused"]["inside_or_after_discovery_window"], 2)
        self.assertEqual(out["refused"]["signal_field_null"], 1)
        self.assertEqual(out["refused"]["no_ticker"], 1)
        self.assertEqual(out["refused"]["no_reported_date"], 1)


class NonOverlapGuard(unittest.TestCase):
    def test_disjoint_passes_and_reports_gap(self):
        conf = s13c.admissible_confirmation_events(
            [_payload("c1", "2024-11-29")])["events"]
        disc = [_payload("d1", "2024-12-02"), _payload("d2", "2026-08-07")]
        sep = s13c.assert_non_overlap(conf, disc)
        self.assertEqual(sep["discovery_interval"],
                         ["2024-12-02", "2026-08-07"])
        self.assertEqual(sep["confirmation_interval"],
                         ["2024-11-29", "2024-11-29"])
        self.assertEqual(sep["gap_days"], 3)
        self.assertEqual(sep["shared_event_ids"], 0)

    def test_shared_id_raises(self):
        conf = s13c.admissible_confirmation_events(
            [_payload("same", "2024-11-29")])["events"]
        disc = [_payload("same", "2024-12-02")]
        with self.assertRaises(ValueError):
            s13c.assert_non_overlap(conf, disc)

    def test_date_overlap_raises(self):
        conf = [{"id": "x", "reported": "2024-12-05", "ticker": "AAA",
                 "signal": 1.0}]
        disc = [_payload("d1", "2024-12-02")]
        with self.assertRaises(ValueError):
            s13c.assert_non_overlap(conf, disc)


class SurvivorshipIdentity(unittest.TestCase):
    def test_effective_ticker_and_delisted_retention(self):
        # the REAL owner (IdentityStore.historical_universe_on) returns rows
        # with delisting_date but NO is_current key — mirror that shape here
        universe = {
            "2024-06-03": [
                {"norgate_assetid": "111", "security_id": "ngid:111",
                 "ticker": "AAA-202409", "ticker_effective_on": "AAA",
                 "delisting_date": "2024-09-30"},
                {"norgate_assetid": "222", "security_id": "ngid:222",
                 "ticker": "BBB", "ticker_effective_on": "BBB",
                 "delisting_date": None},
            ]}
        events = [
            {"id": "1", "reported": "2024-06-03", "ticker": "AAA",
             "signal": 2.0},
            {"id": "2", "reported": "2024-06-03", "ticker": "BBB",
             "signal": 1.0},
            {"id": "3", "reported": "2024-06-03", "ticker": "ZZZ",
             "signal": 0.5},
        ]
        res = s13c.resolve_events(events, lambda d: universe.get(d, []))
        self.assertEqual(res["resolved"], 2)
        self.assertEqual(res["unresolved"], 1)
        self.assertEqual(res["distinct_delisted_securities"], 1)
        self.assertEqual(res["events_delisted_securities"], 1)
        by_id = {e["id"]: e for e in res["events"]}
        self.assertEqual(by_id["1"]["assetid"], "111")   # delisted retained
        self.assertFalse(by_id["1"]["is_current_security"])


class PitExecutionAndMaturation(unittest.TestCase):
    def _sessions(self, n, start="2024-01-01"):
        import datetime
        d = datetime.date.fromisoformat(start)
        out = []
        while len(out) < n:
            if d.weekday() < 5:
                out.append(d.isoformat())
            d += datetime.timedelta(days=1)
        return out

    def test_entry_strictly_after_and_no_early_maturation(self):
        sessions = self._sessions(30)
        panel = {"111": [(d, 100.0 + i) for i, d in enumerate(sessions)]}
        ev = [{"id": "1", "reported": sessions[2], "ticker": "AAA",
               "signal": 1.0, "assetid": "111"}]
        out = s13c.attach_forward_returns(ev, panel, sessions)
        self.assertEqual(len(out), 1)
        row = out[0]
        # entry = first session STRICTLY after the report date
        self.assertEqual(row["entry_session"], sessions[3])
        self.assertGreater(row["entry_session"], row["reported"])
        # 5 and 20 mature inside 30 sessions; 63 must be ABSENT (not
        # substituted, not matured early)
        self.assertIn("fwd_5", row)
        self.assertIn("fwd_20", row)
        self.assertNotIn("fwd_63", row)

    def test_missing_price_path_excluded(self):
        sessions = self._sessions(30)
        panel = {"111": [(sessions[3], 100.0)]}   # entry only; no exits
        ev = [{"id": "1", "reported": sessions[2], "ticker": "AAA",
               "signal": 1.0, "assetid": "111"}]
        self.assertEqual(s13c.attach_forward_returns(ev, panel, sessions), [])


class FrozenStatistic(unittest.TestCase):
    def _events(self, month, n, spread):
        """n events in one month cohort; top-signal names get +spread return."""
        out = []
        for i in range(n):
            sig = float(i)
            fwd = spread if i >= n - n // 5 else 0.0
            out.append({"id": "%s-%d" % (month, i),
                        "reported": "%s-15" % month, "ticker": "T%03d" % i,
                        "signal": sig, "fwd_63": fwd, "fwd_5": 0.0,
                        "fwd_20": 0.0})
        return out

    def test_cohort_floor_and_cost(self):
        events = self._events("2024-01", 24, 0.05)   # below the 25 floor
        self.assertEqual(s13c.cohort_spreads(events, 63), [])
        events = self._events("2024-01", 25, 0.05)
        sp = s13c.cohort_spreads(events, 63)
        self.assertEqual(len(sp), 1)
        self.assertAlmostEqual(sp[0][1], 0.05)
        stats = s13c.horizon_stats(events, 63)
        self.assertAlmostEqual(stats["gross_spread_mean"], 0.05)
        # 25 bps/side -> exactly 50 bps off the spread; never less
        self.assertAlmostEqual(stats["net_spread_mean"], 0.045)

    def test_overlap_adjusted_uses_reduced_dof(self):
        events = []
        for k in range(12):
            events += self._events("2024-%02d" % (k + 1), 25,
                                   0.05 + 0.001 * k)
        stats = s13c.horizon_stats(events, 63)
        self.assertEqual(stats["cohorts"], 12)
        self.assertEqual(stats["effective_independent_cohorts"], 4)
        self.assertLess(stats["t_stat_overlap_adjusted"], stats["t_stat"])


class DispositionLogic(unittest.TestCase):
    def _primary(self, **kw):
        base = {"cohorts": 24, "events_used": 2000, "gross_spread_mean": 0.05,
                "net_spread_mean": 0.045, "t_stat": 3.0,
                "t_stat_overlap_adjusted": 2.1, "hit_rate": 0.7,
                "half1_net_mean": 0.02, "half2_net_mean": 0.02}
        base.update(kw)
        return base

    def _meta(self):
        return {"distinct_delisted_securities": 5}

    def test_confirmed_only_when_all_gates_pass(self):
        v = s13c.decide(self._primary(), self._meta())
        self.assertEqual(v["disposition"], s13c.DISP_CONFIRMED)
        self.assertTrue(v["no_champion_promotion"])

    def test_insufficient_history(self):
        v = s13c.decide(self._primary(cohorts=10), self._meta())
        self.assertEqual(v["disposition"], s13c.DISP_INSUFFICIENT)
        v = s13c.decide(self._primary(events_used=500), self._meta())
        self.assertEqual(v["disposition"], s13c.DISP_INSUFFICIENT)

    def test_partial_when_significance_fails(self):
        v = s13c.decide(self._primary(t_stat=1.5), self._meta())
        self.assertEqual(v["disposition"], s13c.DISP_PARTIAL)

    def test_partial_when_subperiod_fails(self):
        v = s13c.decide(self._primary(half2_net_mean=-0.01), self._meta())
        self.assertEqual(v["disposition"], s13c.DISP_PARTIAL)

    def test_adverse(self):
        v = s13c.decide(self._primary(gross_spread_mean=-0.05,
                                      net_spread_mean=-0.055, t_stat=-2.5,
                                      t_stat_overlap_adjusted=-1.5,
                                      hit_rate=0.2, half1_net_mean=-0.02,
                                      half2_net_mean=-0.03), self._meta())
        self.assertEqual(v["disposition"], s13c.DISP_ADVERSE)

    def test_null(self):
        v = s13c.decide(self._primary(gross_spread_mean=0.001,
                                      net_spread_mean=-0.004, t_stat=0.2,
                                      t_stat_overlap_adjusted=0.1,
                                      hit_rate=0.5, half1_net_mean=-0.001,
                                      half2_net_mean=-0.001), self._meta())
        self.assertEqual(v["disposition"], s13c.DISP_NULL)


class RankHelpers(unittest.TestCase):
    def test_spearman_perfect_and_inverse(self):
        self.assertAlmostEqual(s13c.spearman([1, 2, 3, 4], [10, 20, 30, 40]),
                               1.0)
        self.assertAlmostEqual(s13c.spearman([1, 2, 3, 4], [40, 30, 20, 10]),
                               -1.0)

    def test_partial_fully_explained_is_degenerate_none(self):
        # y IS the control; after partialling nothing remains — the helper
        # must refuse (None), never fabricate a correlation from zero variance
        x = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        ctrl = [2.0, 1.0, 4.0, 3.0, 6.0, 5.0, 8.0, 7.0]
        self.assertIsNone(s13c.partial_spearman(list(ctrl), x, [ctrl]))

    def test_partial_recovers_masked_relationship(self):
        # y = x + strong orthogonal control component; controlling for it
        # must STRENGTHEN the measured y~x rank relationship
        x = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        ctrl = [1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0]
        y = [x[i] + 10.0 * ctrl[i] for i in range(8)]
        raw = s13c.spearman(y, x)
        part = s13c.partial_spearman(y, x, [ctrl])
        self.assertIsNotNone(part)
        self.assertGreater(part, raw)
        self.assertGreater(part, 0.95)


if __name__ == "__main__":
    unittest.main()
