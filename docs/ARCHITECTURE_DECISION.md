# Architecture Decision Document

## 1. Concurrency and Distribution — What I Chose and Why

### The Decision: Database-First State Machine
**Decision:** One transactional store is the single source of truth; dialer workers are completely stateless and interchangeable.

SQLite (with `BEGIN IMMEDIATE` and WAL mode) stands in for PostgreSQL in this prototype. There is no Redis, no Kafka, and no external cache in front of it.

| Problem | Mechanism |
|---|---|
| **Two workers see the same AVAILABLE agent** | Reservation is `UPDATE agents SET state='RESERVED' ... WHERE id=? AND state='AVAILABLE'` inside the same transaction that selects it; loser gets `rowcount == 0`. Backstop: partial unique index `ux_call_agent_live` (one live call per agent). Tested with 16 threads. (On Postgres: `SELECT ... FOR UPDATE SKIP LOCKED` or the same conditional UPDATE). |
| **Same borrower dialled twice** | Borrower flips `READY -> IN_CALL` in the same txn that creates the call; backstop `ux_call_borrower_live`; `idem_key = borrower:dial_seq` is UNIQUE so a duplicate job cannot create a second call. |
| **Predictive over-dial across workers** | The in-flight cap is **re-checked inside the allocation transaction** (check-and-reserve), not only at decision time. 8 workers asking for 100 each with 10 agents $\rightarrow$ exactly 40 calls. |
| **Duplicate / out-of-order provider events** | `provider_events(provider, event_id)` PK dedupes; transitions are rank-monotonic so even a new id for a stale event is a no-op. |
| **Crash after ANSWERED** | The event insert and the state change are **one transaction**. Crash before commit $\rightarrow$ nothing happened and the provider redelivers (we ack only after commit). Crash after commit $\rightarrow$ redelivery is a `DUPLICATE`. |
| **Crash between reserve and dial** | `sweep()` finds `RESERVED` calls older than `reserve_timeout` and re-drives them. Dial is idempotent on `client_ref = call_id`, and the pool first asks every provider `has_call(client_ref)`, so a call that did reach the provider is never placed twice. Events carry our `client_ref`, so they correlate even if we never stored the provider id. |
| **Lost events** | `sweep()` fails calls stuck in INITIATED/RINGING (`setup_timeout`) or CONNECTED (`max_call_seconds`), releasing agent + borrower. |
| **Stale state ("DB says AVAILABLE, cache says RESERVED")** | There is no cache of agent state. The DB row wins, always. Agent release is *fenced* by `call_id` (`WHERE id=? AND call_id=?`) so a late event cannot free an agent already on another call. |
| **Retries** | Provider never accepted $\rightarrow$ requeue after short backoff, **no attempt consumed**. Reached borrower and failed/no answer $\rightarrow$ attempt consumed, exponential backoff, give up after `max_attempts`. Abandoned $\rightarrow$ priority call-back. |
| **Provider timeout ambiguity** | A timeout does not prove the call doesn't exist. We never fail over a timed-out call to the other provider (which could cause double-dialing); we best-effort cancel, fail the call, and if a ghost call is later answered we hang up and count it as an abandon. A definitive 5xx may be retried on the other provider. |

### Why not Redis/Kafka?
The hard invariants (agent held by at most one call, borrower in at most one live call, compliance cap) are multi-row constraints that require a single atomic decision. A relational DB provides that natively. Splitting state across Redis + DB creates the classic "which one wins?" split-brain hazard. Kafka only pays off once event volume exceeds what one database can absorb; it makes exactly-once delivery harder.

---

## 2. Scale Analysis: What Breaks First as Agents Grow

### 1. 100 $\rightarrow$ 1,000 agents:
Nothing breaks; a single PostgreSQL instance handles this with ease.

### 2. ~1,000 $\rightarrow$ 10,000 agents: The per-tick global aggregates
Every tick does `COUNT ... GROUP BY` over all agents and a scan of recent calls for stats, per worker. That is $O(\text{agents})$ work repeated by every worker, contending with writers.
- **The Fix:** Maintain state counts incrementally (agent-state and in-flight counts updated in the same transaction as the transition; rolling answer/abandon counters in a small summary table or memory), and appoint **one pacing leader per shard** (lease with fencing token) instead of $N$ workers all pacing the same pool.

### 3. 10,000+ agents: The single hot allocation path + writer contention
All workers contend for the same `AVAILABLE` rows and the head of the borrower queue; serializable writes queue behind each other.
- **The Fix:** Shard by campaign / agent pool (an agent belongs to exactly one shard, so invariants remain shard-local without cross-shard distributed transactions). Claim borrowers with `FOR UPDATE SKIP LOCKED` in batches. Partition the event ingestion stream by `call_id` (preserving per-call ordering).

### 4. At 100,000 agents:
Shard count and telecom provider trunk limits (Calls Per Second / CPS rate limits) become the primary constraint, not dialer database performance.

---

## 3. Least Confident About

1. **Campaign-wide answer rate:** If answer rates differ strongly by borrower segment or time-of-day, a mixed pool can mislead the estimator. Per-segment estimates would be safer.
2. **Outage visibility:** While a provider is completely silent or unreachable, answers cannot be observed in real time. The hard cap bounds the damage, but cannot eliminate it entirely.
3. **Connected agents finishing soon:** Capacity calculations currently count available agents and agents in wrap-up, but omit connected agents whose talk time is nearing completion. Modeling talk-time hazards would improve utilization further.

---

## 4. Final Question Answer

> **"How would you build a SmartDialer that gets as much of the utilization benefit of predictive dialing as possible, while retaining the deterministic safety characteristics of progressive dialing?"**

Make safety a **hard, state-based invariant** and treat predictive pacing purely as an optimization within those bounds:

1. **Pacing is an untrusted advisor:** The pacing algorithm can only propose a request (`PacingRequest`), never execute dials.
2. **Independent, unbypassable controller:** A separate `SafetyController` translates the request using hard constraints:
   - A deterministic capacity-based cap ($M \times \text{agents available or freeing within call horizon} - \text{in-flight}$).
   - A live abandon rate compliance budget ($\le 3\%$) that trips fallback to progressive with cooldown hysteresis.
   - Circuit-breaker health gates and cold-start gates.
3. **Atomic reservation in one store:** Borrower and agent reservations happen in atomic Compare-And-Swap database transactions. Even across crashes, duplicate events, and multiple concurrent workers, invariants cannot be violated.
4. **Pessimistic estimation:** Use a pessimistic upper-bound estimate (e.g. Beta posterior mean + 1.5 standard deviations) so errors default to idle time rather than illegal abandonments. Every decision is logged with its mathematical rationale for transparency.
