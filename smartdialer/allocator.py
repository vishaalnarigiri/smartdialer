"""Call allocator: turns an ApprovedDispatch into reservations, then hands calls to the provider pool."""
from .providers import ProviderUnavailable
from .safety import verify


class CallAllocator:
    def __init__(self, store, pool, policy):
        self.store, self.pool, self.policy = store, pool, policy

    def execute(self, dispatch, now):
        d = verify(dispatch)                                   # refuses anything not minted by the Safety Controller
        calls = self.store.allocate(d.count, d.mode, now, self.policy)   # atomic reserve (borrower [+ agent])
        for call_id, phone in calls:
            self.dial(call_id, phone, now)
        return calls

    def dial(self, call_id, phone, now):
        try:
            name, _ = self.pool.place(call_id, phone, now)
            self.store.mark_initiated(call_id, name, now)
            return True
        except ProviderUnavailable:
            self.store.dial_failed(call_id, "PROVIDER_ERROR", now)
            return False
