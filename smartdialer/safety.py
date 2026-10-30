"""Safety Controller: the only component that can mint an ApprovedDispatch, the only thing the allocator accepts."""
import math
from dataclasses import dataclass

from .models import PROGRESSIVE, PREDICTIVE, Policy

_MINT = object()   # private capability; only SafetyController.review can attach it


@dataclass(frozen=True)
class ApprovedDispatch:
    count: int
    mode: str            # mode the allocator must use (progressive => reserves an agent per call)
    _mint: object = None


def verify(d):
    if not isinstance(d, ApprovedDispatch) or d._mint is not _MINT:
        raise PermissionError("allocator only accepts dispatches approved by the SafetyController")
    return d


@dataclass(frozen=True)
class Decision:
    action: str          # APPROVE | REDUCE | REJECT | FALLBACK_PROGRESSIVE
    requested: int
    approved: int
    reason: str
    dispatch: ApprovedDispatch = None


class SafetyController:
    def __init__(self, policy: Policy):
        self.policy = policy
        self.fallback = False
        self.fallback_since = None

    def _abandon_state(self, stats, now):
        p = self.policy
        base = max(stats.answered_w, p.abandon_min_sample)
        trip = stats.abandoned_w > 0.8 * p.abandon_limit * base
        clear = stats.abandoned_w <= 0.4 * p.abandon_limit * base
        if not self.fallback and trip:
            self.fallback, self.fallback_since = True, now
        elif self.fallback and clear and now - self.fallback_since >= p.fallback_cooldown:
            self.fallback = False
        return base

    def review(self, req, snap, stats, health, now) -> Decision:
        p = self.policy
        self._abandon_state(stats, now)
        mk = lambda action, n, mode, why: Decision(action, req.requested, n, why,
                                                   ApprovedDispatch(n, mode, _MINT) if n > 0 else None)
        if req.requested <= 0:
            return mk("IDLE", 0, req.mode, "nothing requested (no spare agents / borrowers)")
        if not health["any_available"]:
            return mk("REJECT", 0, req.mode, "all providers unavailable (circuit open)")
        headroom = max(0, snap.available - snap.unbound_inflight)   # agents not already spoken for
        degraded = health["failure_rate"] > p.degraded_failure_rate
        warming = req.mode == PREDICTIVE and len(stats.outcomes) < p.min_calibration_samples
        if req.mode == PROGRESSIVE or self.fallback or degraded or warming:
            n = min(req.requested, headroom, p.max_burst)
            if req.mode == PREDICTIVE:
                why = ("abandon budget exhausted -> progressive" if self.fallback else
                       f"provider failure rate {health['failure_rate']:.0%} -> progressive" if degraded else
                       f"answer rate not yet calibrated ({len(stats.outcomes)}/{p.min_calibration_samples} samples) -> progressive")
                return mk("FALLBACK_PROGRESSIVE" if n > 0 else "REJECT", n, PROGRESSIVE, why)
            return mk("APPROVE" if n == req.requested else ("REDUCE" if n > 0 else "REJECT"), n, PROGRESSIVE,
                      f"1 call per available agent (headroom={headroom})")
        capacity = snap.available + snap.free_soon
        cap_total = math.ceil(p.max_ratio * capacity)
        room = cap_total - snap.unbound_inflight
        n = max(0, min(req.requested, room, p.max_burst))
        if n == req.requested:
            return mk("APPROVE", n, PREDICTIVE, f"within cap {cap_total} (ratio<= {p.max_ratio})")
        if n > 0:
            return mk("REDUCE", n, PREDICTIVE, f"capped by max in-flight {cap_total} = {p.max_ratio} x capacity {capacity}, "
                                              f"{snap.unbound_inflight} already in flight")
        return mk("REJECT", 0, PREDICTIVE, f"in-flight cap reached ({snap.unbound_inflight} >= {cap_total})")
