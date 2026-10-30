"""Pacing engines. They only produce a *request* (a number + explanation). They hold no reference to the
allocator or any provider, so they have no way to place a call or to bypass the Safety Controller."""
import math
from dataclasses import dataclass, field

from .models import PROGRESSIVE, PREDICTIVE


@dataclass(frozen=True)
class PacingRequest:
    mode: str
    requested: int
    explanation: str
    inputs: dict = field(default_factory=dict)


class ProgressivePacer:
    """One available agent -> at most one call."""
    def request(self, snap, stats, now):
        n = max(0, min(snap.available - snap.unbound_inflight, snap.ready_borrowers))
        return PacingRequest(PROGRESSIVE, n, f"progressive: {snap.available} available agents, "
                             f"{snap.unbound_inflight} unbound calls already in flight -> {n}",
                             {"available": snap.available})


class PredictivePacer:
    """Dial enough calls that the *expected* number of answers by the time they connect equals the
    number of agents who will be free by then, using a deliberately pessimistic answer rate.

        A      = available agents + agents finishing wrap-up within `horizon`
        p_up   = upper credible bound of the recent answer rate (Beta posterior, recency-weighted)
        target = floor(A / p_up)         # calls we want live in total
        new    = target - already ringing (unbound)

    Using the UPPER bound of p means we assume more people will answer than we think -> we dial fewer.
    The mistake that causes abandoned calls is *under*-estimating p, so we bias away from it.
    """
    def __init__(self, z=1.5, prior=(1.5, 3.5), half_life=40.0, min_p=0.05):
        self.z, self.prior, self.half_life, self.min_p = z, prior, half_life, min_p

    def estimate(self, outcomes):
        a0, b0 = self.prior
        n_eff = s = 0.0
        for i, y in enumerate(outcomes):                  # newest first
            w = 0.5 ** (i / self.half_life)
            n_eff += w
            s += w * y
        a, b = a0 + s, b0 + n_eff - s
        mean = a / (a + b)
        sd = math.sqrt(a * b / ((a + b) ** 2 * (a + b + 1)))
        return mean, min(1.0, mean + self.z * sd), n_eff

    def request(self, snap, stats, now):
        mean, p_up, n_eff = self.estimate(stats.outcomes)
        A = snap.available + snap.free_soon
        target = math.floor(A / max(p_up, self.min_p))
        n = max(0, target - snap.unbound_inflight)
        n = min(n, snap.ready_borrowers)
        why = (f"predictive: A={A} (avail {snap.available} + {snap.free_soon} freeing <= horizon), "
               f"p_hat={mean:.2f} p_upper={p_up:.2f} (n_eff={n_eff:.0f}), target=floor({A}/{p_up:.2f})={target}, "
               f"in-flight={snap.unbound_inflight} -> request {n}")
        return PacingRequest(PREDICTIVE, n, why, {"A": A, "p_hat": mean, "p_upper": p_up, "target": target})
