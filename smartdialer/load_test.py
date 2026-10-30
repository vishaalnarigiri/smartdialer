"""Basic load test.  python -m smartdialer.load_test [--agents 1000]
Phase 1: N threads fight over N agents (file-backed SQLite, WAL): correctness + throughput.
Phase 2: event-apply throughput (with 30% duplicates).
Phase 3: per-tick control-loop latency at 100 / 1,000 / 10,000 agents: what grows first.
"""
import argparse
import os
import tempfile
import threading
import time

from .models import Policy, PREDICTIVE, PROGRESSIVE
from .pacing import PredictivePacer
from .store import Store

P = Policy()


def mk(path, agents, borrowers):
    s = Store(path).configure(P)
    ids = [f"a{i}" for i in range(agents)]
    s.add_agents(ids)
    for a in ids:
        s.agent_login(a, 0)
    s.add_borrowers([(f"b{i}", f"+91{i}") for i in range(borrowers)])
    return s


def phase1(agents, workers=8):
    path = os.path.join(tempfile.mkdtemp(), "load.db")
    s = mk(path, agents, agents * 3)
    got, lock, ops = [], threading.Lock(), [0]

    def w():
        while True:
            r = s.allocate(10, PROGRESSIVE, 0, P)
            with lock:
                got.extend(r); ops[0] += 1
            if not r:
                return
    t0 = time.time()
    ts = [threading.Thread(target=w) for _ in range(workers)]
    [t.start() for t in ts]; [t.join() for t in ts]
    dt = time.time() - t0
    assert len(got) == agents and not s.check_invariants(), "double allocation!"
    print(f"[1] {workers} workers, {agents} agents: reserved {len(got)} (== agents, 0 double-reservations) "
          f"in {dt:.2f}s = {len(got) / dt:,.0f} reservations/s")
    return s, got


def phase2(s, calls):
    t0, n = time.time(), 0
    for cid, _ in calls:
        for typ in ("RINGING", "ANSWERED", "ANSWERED", "COMPLETED"):      # one dup per call
            s.apply_event("L", f"{cid}:{typ}", cid, typ, None, 1.0 if typ != "COMPLETED" else 60.0); n += 1
    dt = time.time() - t0
    assert not s.check_invariants()
    print(f"[2] applied {n} events (25% duplicates) in {dt:.2f}s = {n / dt:,.0f} events/s, invariants hold")


def phase3():
    print("[3] control-loop cost per tick (snapshot + stats + pacing + allocate 1 batch)")
    for n in (100, 1000, 10000):
        s = mk(":memory:", n, n * 5)
        pacer, ts = PredictivePacer(), []
        for i in range(5):
            t0 = time.time()
            snap = s.snapshot(i, P.horizon)
            st = s.stats(i, P)
            pacer.request(snap, st, i)
            t1 = time.time()
            s.allocate(min(n // 10, 200), PREDICTIVE, i, P)
            ts.append((t1 - t0, time.time() - t1))
        a = sum(x for x, _ in ts) / len(ts); b = sum(y for _, y in ts) / len(ts)
        print(f"    {n:>6} agents: read side {a * 1000:7.1f} ms | allocate txn {b * 1000:7.1f} ms")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--agents", type=int, default=1000)
    a = ap.parse_args()
    s, got = phase1(a.agents)
    phase2(s, got)
    phase3()
