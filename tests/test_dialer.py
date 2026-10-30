import os
import random
import tempfile
import threading
import unittest

from smartdialer.allocator import CallAllocator
from smartdialer.engine import Dialer
from smartdialer.models import Policy, PROGRESSIVE, PREDICTIVE
from smartdialer.pacing import PredictivePacer, ProgressivePacer, PacingRequest
from smartdialer.providers import ProviderPool, World, provider_a, provider_b, ProviderUnavailable
from smartdialer.safety import ApprovedDispatch, SafetyController
from smartdialer.store import Snapshot, Stats, Store

P = Policy()


def mkstore(agents=3, borrowers=10, path=":memory:"):
    s = Store(path).configure(P)
    ids = [f"a{i}" for i in range(agents)]
    s.add_agents(ids)
    for a in ids:
        s.agent_login(a, 0)
    s.add_borrowers([(f"b{i}", f"+91{i}") for i in range(borrowers)])
    return s


def ev(s, call, typ, now=1.0, eid=None, reason=None):
    return s.apply_event("T", eid or f"{call}:{typ}", call, typ, reason, now)


def agent_state(s, a):
    return s.one("SELECT state FROM agents WHERE id=?", (a,))


def call_state(s, c):
    return s.one("SELECT state FROM calls WHERE id=?", (c,))


class TestConcurrency(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.path = os.path.join(self.d, "t.db")

    def run_threads(self, n, fn):
        ts = [threading.Thread(target=fn) for _ in range(n)]
        [t.start() for t in ts]
        [t.join() for t in ts]

    def test_two_workers_one_agent(self):
        s = mkstore(agents=1, borrowers=20, path=self.path)
        got = []
        self.run_threads(8, lambda: got.extend(s.allocate(1, PROGRESSIVE, 0, P)))
        self.assertEqual(len(got), 1)
        self.assertEqual(s.check_invariants(), [])

    def test_progressive_never_exceeds_agents(self):
        s = mkstore(agents=50, borrowers=500, path=self.path)
        got = []
        self.run_threads(16, lambda: [got.extend(s.allocate(7, PROGRESSIVE, 0, P)) for _ in range(10)])
        self.assertEqual(len(got), 50)
        self.assertEqual(len({c for c, _ in got}), 50)
        self.assertEqual(s.check_invariants(), [])

    def test_predictive_cap_is_enforced_inside_the_transaction(self):
        s = mkstore(agents=10, borrowers=500, path=self.path)
        got = []
        self.run_threads(8, lambda: got.extend(s.allocate(100, PREDICTIVE, 0, P)))
        self.assertEqual(len(got), 40)            # ceil(4.0 * 10) no matter how many workers asked
        self.assertEqual(s.check_invariants(), [])


class TestCallStateMachine(unittest.TestCase):
    def setUp(self):
        self.s = mkstore(agents=1, borrowers=3)
        [(self.c, _)] = self.s.allocate(1, PROGRESSIVE, 0, P)
        self.s.mark_initiated(self.c, "T", 0.5)

    def test_duplicate_events_cause_one_transition(self):
        s, c = self.s, self.c
        outs = [ev(s, c, "RINGING"), ev(s, c, "ANSWERED"), ev(s, c, "ANSWERED"),            # same id -> DUPLICATE
                ev(s, c, "ANSWERED", eid="other-id"), ev(s, c, "COMPLETED", now=40)]        # new id, stale -> ignored
        self.assertEqual(outs, ["APPLIED", "APPLIED", "DUPLICATE", "IGNORED_STALE", "APPLIED"])
        self.assertEqual(call_state(s, c), "COMPLETED")
        self.assertEqual(agent_state(s, "a0"), "WRAP_UP")
        self.assertEqual(s.one("SELECT attempts FROM borrowers WHERE id='b0'"), 1)
        self.assertEqual(ev(s, c, "COMPLETED", eid="again"), "IGNORED_TERMINAL")
        self.assertEqual(s.one("SELECT attempts FROM borrowers WHERE id='b0'"), 1)      # not double counted
        self.assertEqual(s.check_invariants(), [])

    def test_out_of_order_completed_first(self):
        s, c = self.s, self.c
        self.assertEqual(ev(s, c, "COMPLETED", now=30), "APPLIED_OUT_OF_ORDER")
        self.assertEqual(ev(s, c, "ANSWERED"), "IGNORED_TERMINAL")
        self.assertEqual(ev(s, c, "RINGING"), "IGNORED_TERMINAL")
        self.assertEqual(call_state(s, c), "COMPLETED")
        self.assertEqual(agent_state(s, "a0"), "AVAILABLE")      # agent released, never stuck
        self.assertEqual(s.check_invariants(), [])

    def test_ringing_after_answered_is_stale(self):
        s, c = self.s, self.c
        ev(s, c, "ANSWERED")
        self.assertEqual(ev(s, c, "RINGING"), "IGNORED_STALE")
        self.assertEqual(call_state(s, c), "CONNECTED")

    def test_worker_crash_right_after_answered_is_atomic(self):
        s, c = self.s, self.c
        orig = s._transition

        def boom(*a, **k):
            orig(*a, **k)
            raise RuntimeError("worker died")
        s._transition = boom
        with self.assertRaises(RuntimeError):
            ev(s, c, "ANSWERED")
        s._transition = orig
        self.assertEqual(call_state(s, c), "INITIATED")                       # txn rolled back wholesale
        self.assertEqual(s.one("SELECT COUNT(*) FROM provider_events"), 0)    # ...including the dedupe row
        self.assertEqual(ev(s, c, "ANSWERED"), "APPLIED")                     # provider redelivers
        self.assertEqual(ev(s, c, "COMPLETED", now=50), "APPLIED")
        self.assertEqual(call_state(s, c), "COMPLETED")
        self.assertEqual(s.check_invariants(), [])

    def test_no_answer_retries_with_backoff_then_gives_up(self):
        s, c = self.s, self.c
        ev(s, c, "RINGING")
        ev(s, c, "FAILED", now=25, reason="NO_ANSWER")
        b = s.q("SELECT * FROM borrowers WHERE id='b0'")[0]
        self.assertEqual((b["state"], b["attempts"], b["next_attempt_at"]), ("READY", 1, 25 + 30))
        self.assertEqual(agent_state(s, "a0"), "AVAILABLE")
        for i in range(2):                                   # attempts 2 and 3
            s.q("UPDATE borrowers SET next_attempt_at=0 WHERE id='b0'")
            s.q("UPDATE borrowers SET priority=99 WHERE id='b0'")
            [(c2, _)] = s.allocate(1, PROGRESSIVE, 100 + i, P)
            ev(s, c2, "FAILED", now=110 + i, reason="NO_ANSWER")
        self.assertEqual(s.one("SELECT state FROM borrowers WHERE id='b0'"), "DONE")

    def test_dial_failure_does_not_burn_an_attempt(self):
        s, c = self.s, self.c
        s.dial_failed(c, "PROVIDER_ERROR", 1)
        self.assertEqual(s.one("SELECT attempts FROM borrowers WHERE id='b0'"), 0)
        self.assertEqual(agent_state(s, "a0"), "AVAILABLE")


class TestPredictiveBinding(unittest.TestCase):
    def test_answer_with_no_free_agent_is_abandoned_and_called_back(self):
        s = mkstore(agents=1, borrowers=5)
        calls = s.allocate(3, PREDICTIVE, 0, P)
        for c, _ in calls:
            s.mark_initiated(c, "T", 0.5)
        outs = [ev(s, c, "ANSWERED", now=5) for c, _ in calls[:2]]
        self.assertEqual(sorted(outs), ["ABANDONED", "APPLIED"])      # exactly one agent -> exactly one connect
        self.assertEqual(s.one("SELECT COUNT(*) FROM calls WHERE state='CONNECTED'"), 1)
        self.assertEqual(s.one("SELECT priority FROM borrowers WHERE id IN (SELECT borrower_id FROM calls WHERE state='ABANDONED')"), 10)
        self.assertEqual(s.check_invariants(), [])

    def test_ghost_answer_after_write_off_is_reported_for_hangup(self):
        s = mkstore(agents=1, borrowers=2)
        [(c, _)] = s.allocate(1, PREDICTIVE, 0, P)
        s.dial_failed(c, "PROVIDER_ERROR", 1)                 # we timed out and gave up...
        self.assertEqual(ev(s, c, "ANSWERED", now=9), "ORPHAN_ANSWER")   # ...but the call existed
        self.assertEqual(s.stats(10, P).abandoned_w, 1)       # and it counts against the abandon budget


class TestAgentLeaves(unittest.TestCase):
    def test_leave_while_reserved_cancels_call_and_returns_borrower(self):
        s = mkstore(agents=2, borrowers=5)
        [(c, _), _] = s.allocate(2, PROGRESSIVE, 0, P)
        a = s.one("SELECT agent_id FROM calls WHERE id=?", (c,))
        self.assertEqual(s.agent_leave(a, "OFFLINE", 1), [c])
        self.assertEqual(call_state(s, c), "CANCELLED")
        self.assertEqual(agent_state(s, a), "OFFLINE")
        self.assertEqual(s.one("SELECT state FROM borrowers WHERE id=(SELECT borrower_id FROM calls WHERE id=?)", (c,)), "READY")
        self.assertEqual(s.check_invariants(), [])
        self.assertEqual(ev(s, c, "ANSWERED", now=3), "ORPHAN_ANSWER")   # late answer on cancelled call -> hang up

    def test_leave_while_connected_waits_for_wrap_up(self):
        s = mkstore(agents=1, borrowers=2)
        [(c, _)] = s.allocate(1, PROGRESSIVE, 0, P)
        ev(s, c, "ANSWERED")
        s.agent_leave("a0", "OFFLINE", 2)
        self.assertEqual(agent_state(s, "a0"), "CONNECTED")      # conversation is never cut
        ev(s, c, "COMPLETED", now=30)
        s.release_wrapups(30 + P.wrap_seconds)
        self.assertEqual(agent_state(s, "a0"), "OFFLINE")


class TestCrashRecovery(unittest.TestCase):
    def setUp(self):
        self.world = World(lambda t: 1.0, lambda t, r: 30, random.Random(1))
        self.a = provider_a(self.world)
        self.pool = ProviderPool([self.a])
        self.s = mkstore(agents=2, borrowers=5)
        self.d = Dialer(self.s, self.pool, ProgressivePacer(), SafetyController(P), P)

    def test_crash_after_reserve_before_dial(self):
        calls = self.s.allocate(2, PROGRESSIVE, 0, P)                    # ...worker dies here
        self.d.recover(5)                                                # too early: another worker may still be dialling
        self.assertEqual(self.a.stats["placed"], 0)
        self.d.recover(20)                                               # a surviving worker re-drives
        self.assertEqual(self.a.stats["placed"], 2)
        self.assertEqual({call_state(self.s, c) for c, _ in calls}, {"INITIATED"})

    def test_crash_after_dial_before_mark_initiated_never_double_dials(self):
        [(c, phone)] = self.s.allocate(1, PROGRESSIVE, 0, P)
        self.pool.place(c, phone, 0)                                     # provider has it, DB does not know
        self.d.recover(20)
        self.assertEqual(self.a.stats["placed"], 1)                      # idempotent on client_ref
        self.assertEqual(call_state(self.s, c), "INITIATED")

    def test_lost_events_are_swept(self):
        [(c, phone)] = self.s.allocate(1, PROGRESSIVE, 0, P)
        self.s.mark_initiated(c, "A", 0)
        self.d.recover(P.setup_timeout + 1)
        self.assertEqual(call_state(self.s, c), "FAILED")
        self.assertEqual(self.s.check_invariants(), [])
        self.assertEqual(agent_state(self.s, "a0") , "AVAILABLE")


class TestProviders(unittest.TestCase):
    def setUp(self):
        self.world = World(lambda t: 0.5, lambda t, r: 60, random.Random(1))

    def test_breaker_opens_and_traffic_shifts(self):
        a, b = provider_a(self.world), provider_b(self.world)
        a.add_outage(0, 1000)
        pool = ProviderPool([a, b], weights={"A": 5, "B": 1})
        ok = fail = 0
        for i in range(60):
            try:
                pool.place(f"c{i}", "x", 10 + i * 0.1); ok += 1
            except ProviderUnavailable:
                fail += 1
        self.assertFalse(pool.breakers["A"].allow(20))
        self.assertEqual(pool.health(20)["usable"], ["B"])
        self.assertGreater(ok, 40)                       # after the breaker trips everything goes to B
        self.assertLess(fail, 15)                        # only the first few ambiguous timeouts were lost

    def test_all_down_means_safety_rejects(self):
        a = provider_a(self.world); a.add_outage(0, 1000)
        pool = ProviderPool([a])
        for i in range(10):
            try: pool.place(f"c{i}", "x", 1)
            except ProviderUnavailable: pass
        h = pool.health(2)
        self.assertFalse(h["any_available"])
        dec = SafetyController(P).review(PacingRequest(PROGRESSIVE, 5, ""), Snapshot(2, available=5), Stats(), h, 2)
        self.assertEqual((dec.action, dec.approved), ("REJECT", 0))

    def test_provider_b_misbehaves_but_state_stays_consistent(self):
        b = provider_b(self.world, seed=5)
        pool = ProviderPool([b])
        s = mkstore(agents=30, borrowers=400)
        d = Dialer(s, pool, ProgressivePacer(), SafetyController(P), P)
        for t in range(300):
            d.pump_events(t); d.tick(t)
        self.assertGreater(b.stats["dups"], 0)
        self.assertEqual(s.check_invariants(), [])


class TestSafetyAndPacing(unittest.TestCase):
    healthy = {"any_available": True, "usable": ["A"], "failure_rate": 0.0}
    calibrated = Stats(outcomes=[1, 0] * 20)

    def test_predictive_request_is_reduced_to_cap(self):
        dec = SafetyController(P).review(PacingRequest(PREDICTIVE, 100, ""), Snapshot(0, available=10, ready_borrowers=999),
                                         self.calibrated, self.healthy, 0)
        self.assertEqual((dec.action, dec.approved), ("REDUCE", 40))

    def test_progressive_never_exceeds_available(self):
        dec = SafetyController(P).review(PacingRequest(PROGRESSIVE, 50, ""), Snapshot(0, available=7), Stats(), self.healthy, 0)
        self.assertEqual((dec.approved, dec.dispatch.mode), (7, PROGRESSIVE))

    def test_cold_start_falls_back_to_progressive(self):
        dec = SafetyController(P).review(PacingRequest(PREDICTIVE, 30, ""), Snapshot(0, available=5), Stats(outcomes=[1] * 5), self.healthy, 0)
        self.assertEqual((dec.action, dec.approved), ("FALLBACK_PROGRESSIVE", 5))

    def test_abandon_budget_trips_fallback_with_hysteresis(self):
        sc = SafetyController(P)
        snap, req = Snapshot(0, available=10), PacingRequest(PREDICTIVE, 30, "")
        bad = Stats(outcomes=[1, 0] * 20, answered_w=60, abandoned_w=3)
        self.assertEqual(sc.review(req, snap, bad, self.healthy, 100).action, "FALLBACK_PROGRESSIVE")
        good = Stats(outcomes=[1, 0] * 20, answered_w=200, abandoned_w=0)
        self.assertEqual(sc.review(req, snap, good, self.healthy, 110).action, "FALLBACK_PROGRESSIVE")   # cooldown
        self.assertIn(sc.review(req, snap, good, self.healthy, 170).action, ("APPROVE", "REDUCE"))        # recovered

    def test_degraded_provider_forces_progressive(self):
        h = {"any_available": True, "usable": ["B"], "failure_rate": 0.6}
        dec = SafetyController(P).review(PacingRequest(PREDICTIVE, 30, ""), Snapshot(0, available=5), self.calibrated, h, 0)
        self.assertEqual((dec.action, dec.approved), ("FALLBACK_PROGRESSIVE", 5))

    def test_allocator_rejects_forged_dispatch(self):
        s = mkstore()
        alloc = CallAllocator(s, ProviderPool([provider_a(World(lambda t: 1, lambda t, r: 1, random.Random()))]), P)
        for forged in (ApprovedDispatch(5, PREDICTIVE), ApprovedDispatch(5, PREDICTIVE, object()), 5, None):
            with self.assertRaises(PermissionError):
                alloc.execute(forged, 0)
        self.assertEqual(s.one("SELECT COUNT(*) FROM calls"), 0)

    def test_policy_cannot_disable_limits(self):
        with self.assertRaises(ValueError): Policy(abandon_limit=0.2)
        with self.assertRaises(ValueError): Policy(max_ratio=50)

    def test_pacer_math_and_direction(self):
        pc, snap = PredictivePacer(), Snapshot(0, available=10, free_soon=2, ready_borrowers=999)
        low = pc.request(snap, Stats(outcomes=[1] * 10 + [0] * 90), 0)     # ~10% answer rate
        high = pc.request(snap, Stats(outcomes=[1] * 70 + [0] * 30), 0)    # ~70%
        self.assertGreater(low.requested, high.requested)
        self.assertIn("target=floor(12/", high.explanation)
        self.assertGreaterEqual(high.requested, 12)                         # still dials ahead of free agents
        self.assertLessEqual(pc.request(Snapshot(0, available=10, unbound_inflight=500, ready_borrowers=999), Stats(outcomes=[1] * 50), 0).requested, 0)

    def test_pessimistic_bound_exceeds_mean(self):
        m, up, _ = PredictivePacer().estimate([1] * 20 + [0] * 80)
        self.assertGreater(up, m)


class TestSimulationSmoke(unittest.TestCase):
    def test_short_predictive_run_is_consistent_and_beats_progressive(self):
        from smartdialer.sim import run
        pg = run("A (20%, 120s)", PROGRESSIVE, agents=30, duration=400, borrowers=5000)
        pd = run("A (20%, 120s)", PREDICTIVE, agents=30, duration=400, borrowers=5000)
        self.assertGreater(pd["util"], pg["util"])
        self.assertLessEqual(pd["abandon"], 0.03)
        self.assertEqual(pg["abandoned"], 0)          # progressive can never abandon

if __name__ == "__main__":
    unittest.main()
