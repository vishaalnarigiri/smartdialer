"""A dialer worker. Any number can run against the same store; correctness lives in the store, not here."""
from .allocator import CallAllocator


class Dialer:
    def __init__(self, store, pool, pacer, safety, policy, worker_id="w1"):
        self.store, self.pool, self.pacer, self.safety = store.configure(policy), pool, pacer, safety
        self.policy, self.worker_id = policy, worker_id
        self.allocator = CallAllocator(store, pool, policy)

    def tick(self, now):
        self.store.release_wrapups(now)
        self.recover(now)
        snap = self.store.snapshot(now, self.policy.horizon)
        stats = self.store.stats(now, self.policy)
        req = self.pacer.request(snap, stats, now)
        dec = self.safety.review(req, snap, stats, self.pool.health(now), now)
        self.store.log_decision(now, req.mode, dec.requested, dec.approved, dec.action, dec.reason,
                                {"why": req.explanation, "worker": self.worker_id})
        if dec.dispatch:
            self.allocator.execute(dec.dispatch, now)
        return req, dec

    def recover(self, now):
        redrive, cancel, hangup = self.store.sweep(now, self.policy)
        for call_id, phone in redrive:          # idempotent re-drive: provider dedupes on client_ref
            self.allocator.dial(call_id, phone, now)
        for call_id in cancel + hangup:
            self.pool.cancel(None, call_id, now)

    def pump_events(self, now):
        for name, ev in self.pool.poll(now):
            out = self.store.apply_event(name, ev.event_id, ev.call_id, ev.type, ev.reason, now)
            if out in ("ABANDONED", "ORPHAN_ANSWER"):
                self.pool.cancel(name, ev.call_id, now)      # hang up on a borrower we cannot serve
            self.pool.ack(name, [ev.seq])                    # ack AFTER the txn: crash => redelivery => DUPLICATE
