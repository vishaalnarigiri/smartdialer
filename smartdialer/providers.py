"""Telecom provider interface, two mock providers with very different behaviour, and a pool with circuit breakers.

All time is virtual (`now` is passed in) so simulations are deterministic and fast.
"""
import random
from collections import deque
from dataclasses import dataclass


class ProviderError(Exception):
    """Definitive failure: the provider did NOT create the call. Safe to try elsewhere."""


class ProviderTimeout(ProviderError):
    """Ambiguous: the call may or may not exist at the provider. Never fail over blindly."""


class ProviderUnavailable(Exception):
    pass


@dataclass
class Event:
    provider: str
    event_id: str
    call_id: str          # our client_ref: lets us correlate events even if we crashed before saving provider ids
    type: str             # RINGING | ANSWERED | COMPLETED | FAILED
    ts: float
    reason: str = None
    deliver_at: float = 0.0
    seq: int = 0          # delivery handle for ack (distinct from event_id, which is what dedupes)


class TelecomProvider:
    """The only surface the dialer knows about."""
    name = "base"

    def place_call(self, client_ref, phone, now): ...      # idempotent on client_ref; returns provider call id
    def has_call(self, client_ref): ...                    # lookup by client_ref (every real provider has this)
    def cancel(self, client_ref, now): ...                 # best effort, also used as hangup
    def poll_events(self, now, limit=500): ...             # at-least-once delivery
    def ack(self, seqs): ...


class World:
    """What the borrowers do. Providers just carry the calls."""
    def __init__(self, answer_prob, talk_time, rng):
        self.answer_prob, self.talk_time, self.rng = answer_prob, talk_time, rng


@dataclass
class Profile:
    name: str
    setup_latency: tuple      # (lo, hi) seconds before RINGING
    ring_answer: tuple        # seconds from ringing to pick-up
    ring_timeout: float
    place_fail_rate: float = 0.0
    timeout_rate: float = 0.0
    ghost_rate: float = 0.0   # fraction of timeouts where the call WAS created anyway
    dup_rate: float = 0.0
    jitter: float = 0.0       # random extra delivery delay => out-of-order arrival
    slow_rate: float = 0.0    # fraction of events delayed by a lot
    slow_delay: float = 20.0


class MockProvider(TelecomProvider):
    def __init__(self, profile: Profile, world: World, seed=0):
        self.p, self.name, self.world = profile, profile.name, world
        self.rng = random.Random(seed)
        self.calls = {}                 # client_ref -> provider call id
        self._events, self._seq = [], 0
        self.outages = []               # [(start, end)]
        self.stats = dict(placed=0, rejected=0, timeouts=0, ghosts=0, dups=0)

    def add_outage(self, start, end):
        self.outages.append((start, end))

    def _down(self, now):
        return any(s <= now < e for s, e in self.outages)

    def has_call(self, client_ref):
        return self.calls.get(client_ref)

    def place_call(self, client_ref, phone, now):
        if client_ref in self.calls:
            return self.calls[client_ref]
        r = self.rng
        if self._down(now) or r.random() < self.p.timeout_rate:
            self.stats["timeouts"] += 1
            if not self._down(now) and r.random() < self.p.ghost_rate:
                self._create(client_ref, now)
                self.stats["ghosts"] += 1
            raise ProviderTimeout(f"{self.name}: timeout")
        if r.random() < self.p.place_fail_rate:
            self.stats["rejected"] += 1
            raise ProviderError(f"{self.name}: 503")
        return self._create(client_ref, now)

    def _create(self, ref, now):
        r, p = self.rng, self.p
        pcid = f"{self.name}-{len(self.calls) + 1}"
        self.calls[ref] = pcid
        self.stats["placed"] += 1
        t = now + r.uniform(*p.setup_latency)
        evs = [("RINGING", t, None)]
        if r.random() < self.world.answer_prob(now):
            ta = t + r.uniform(*p.ring_answer)
            evs += [("ANSWERED", ta, None), ("COMPLETED", ta + self.world.talk_time(now, r), None)]
        else:
            evs.append(("FAILED", t + p.ring_timeout, "NO_ANSWER"))
        for typ, ts, reason in evs:
            delay = r.uniform(0, p.jitter)
            if r.random() < p.slow_rate:
                delay += p.slow_delay
            self._push(Event(self.name, f"{pcid}:{typ}", ref, typ, ts, reason, ts + delay))
            if r.random() < p.dup_rate:
                self.stats["dups"] += 1
                self._push(Event(self.name, f"{pcid}:{typ}", ref, typ, ts, reason, ts + delay + r.uniform(0, 5)))
        return pcid

    def _push(self, ev):
        self._seq += 1
        ev.seq = self._seq
        self._events.append(ev)

    def cancel(self, client_ref, now):
        if client_ref in self.calls:   # drop anything not yet occurred; what already happened is history
            self._events = [e for e in self._events if not (e.call_id == client_ref and e.ts > now)]

    def poll_events(self, now, limit=500):
        if self._down(now):
            return []                  # outage: events are held back, not lost
        due = sorted((e for e in self._events if e.deliver_at <= now), key=lambda e: e.deliver_at)
        return due[:limit]

    def ack(self, seqs):
        s = set(seqs)
        self._events = [e for e in self._events if e.seq not in s]


def provider_a(world, seed=1):
    """Fast, reliable, clean events."""
    return MockProvider(Profile("A", setup_latency=(0.5, 1.5), ring_answer=(3, 10), ring_timeout=25,
                                place_fail_rate=0.005, jitter=0.2), world, seed)


def provider_b(world, seed=2):
    """Slower, timeouts (some ghost calls), duplicate + out-of-order + occasionally very late events."""
    return MockProvider(Profile("B", setup_latency=(2, 6), ring_answer=(4, 14), ring_timeout=30,
                                place_fail_rate=0.03, timeout_rate=0.04, ghost_rate=0.5, dup_rate=0.15,
                                jitter=4.0, slow_rate=0.03, slow_delay=25), world, seed)


class CircuitBreaker:
    def __init__(self, window=20, min_samples=6, threshold=0.5, cooldown=30.0):
        self.w, self.min, self.th, self.cd = deque(maxlen=window), min_samples, threshold, cooldown
        self.open_until, self.probing = None, False

    def allow(self, now):
        if self.open_until is None:
            return True
        return now >= self.open_until and not self.probing     # half-open: one probe at a time

    def on_attempt(self, now):
        if self.open_until is not None and now >= self.open_until:
            self.probing = True

    def record(self, ok, now):
        if self.probing:
            self.probing = False
            if ok:
                self.open_until = None
                self.w.clear()
            else:
                self.open_until = now + self.cd
            return
        self.w.append(ok)
        if len(self.w) >= self.min and self.failure_rate() >= self.th:
            self.open_until, _ = now + self.cd, self.w.clear()

    def failure_rate(self):
        return (1 - sum(self.w) / len(self.w)) if self.w else 0.0


class ProviderPool:
    """Routing + health. The dialer talks to this, never to a concrete provider."""
    def __init__(self, providers, weights=None, seed=3):
        self.providers = {p.name: p for p in providers}
        self.weights = weights or {p.name: 1.0 for p in providers}
        self.breakers = {p.name: CircuitBreaker() for p in providers}
        self.rng = random.Random(seed)

    def _order(self):
        names = list(self.providers)
        first = self.rng.choices(names, [self.weights[n] for n in names])[0]
        return [first] + [n for n in names if n != first]

    def place(self, client_ref, phone, now):
        for n in self.providers:                          # re-drive after a crash: never double-place
            pcid = self.providers[n].has_call(client_ref)
            if pcid:
                return n, pcid
        last = None
        for n in self._order():
            br, prov = self.breakers[n], self.providers[n]
            if not br.allow(now):
                continue
            br.on_attempt(now)
            try:
                pcid = prov.place_call(client_ref, phone, now)
                br.record(True, now)
                return n, pcid
            except ProviderTimeout as e:                  # ambiguous: don't fail over (could double-dial)
                br.record(False, now)
                prov.cancel(client_ref, now)
                raise ProviderUnavailable(str(e))
            except ProviderError as e:                    # definitive: nothing was created, try next
                br.record(False, now)
                last = e
        raise ProviderUnavailable(str(last) if last else "all providers open-circuit")

    def cancel(self, name, client_ref, now):
        if name in self.providers:
            self.providers[name].cancel(client_ref, now)
        else:
            for p in self.providers.values():
                p.cancel(client_ref, now)

    def poll(self, now):
        return [(n, e) for n, p in self.providers.items() for e in p.poll_events(now)]

    def ack(self, name, seqs):
        self.providers[name].ack(seqs)

    def health(self, now):
        usable = [n for n in self.providers if self.breakers[n].allow(now)]
        fr = max((self.breakers[n].failure_rate() for n in usable), default=1.0)
        return {"any_available": bool(usable), "usable": usable, "failure_rate": fr}
