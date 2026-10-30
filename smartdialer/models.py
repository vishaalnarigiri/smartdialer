"""States, transition ranks and the (immutable) safety policy."""
from dataclasses import dataclass


class AgentState:
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"      # progressive: agent held for a specific call, dial not yet confirmed
    DIALING = "DIALING"        # progressive: provider accepted the call, ringing
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"


class CallState:
    QUEUED = "QUEUED"          # (reserved for future queueing; calls are created as RESERVED)
    RESERVED = "RESERVED"      # borrower (and, in progressive mode, agent) reserved
    INITIATED = "INITIATED"    # provider accepted the request
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"      # borrower picked up (transient: bound to an agent in the same txn)
    CONNECTED = "CONNECTED"    # borrower <-> agent talking
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    ABANDONED = "ABANDONED"    # borrower answered, no agent available  (compliance-relevant)


# Calls only move forward in rank. Terminal states are absorbing.
RANK = {
    CallState.QUEUED: 0, CallState.RESERVED: 1, CallState.INITIATED: 2, CallState.RINGING: 3,
    CallState.ANSWERED: 4, CallState.CONNECTED: 5,
    CallState.COMPLETED: 9, CallState.FAILED: 9, CallState.CANCELLED: 9, CallState.ABANDONED: 9,
}
TERMINAL = (CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED, CallState.ABANDONED)
TERMINAL_SQL = "(" + ",".join(f"'{s}'" for s in TERMINAL) + ")"

PROGRESSIVE = "PROGRESSIVE"
PREDICTIVE = "PREDICTIVE"


@dataclass(frozen=True)
class Policy:
    """Safety/operational limits. Frozen and validated: there is no 'safety off' switch."""
    wrap_seconds: float = 10.0
    horizon: float = 12.0              # how far ahead we count agents that are about to free up
    reserve_timeout: float = 15.0      # RESERVED but never confirmed with provider -> re-drive
    setup_timeout: float = 60.0        # INITIATED/RINGING with no events -> give up
    max_call_seconds: float = 1800.0   # CONNECTED with no COMPLETED -> reconcile/close
    max_attempts: int = 3
    retry_backoff: float = 30.0
    max_ratio: float = 4.0             # hard cap: in-flight unbound calls <= ratio * (available + free_soon)
    max_burst: int = 200               # hard cap per tick
    abandon_limit: float = 0.03        # compliance ceiling (fraction of answered calls)
    abandon_window: float = 300.0
    abandon_min_sample: int = 50
    fallback_cooldown: float = 60.0
    degraded_failure_rate: float = 0.30
    min_calibration_samples: int = 30   # predictive stays progressive until the answer rate is measured

    def __post_init__(self):
        if not 1.0 <= self.max_ratio <= 5.0:
            raise ValueError("max_ratio must be within [1, 5]")
        if not 0 < self.abandon_limit <= 0.03:
            raise ValueError("abandon_limit may not exceed the 3% compliance ceiling")
