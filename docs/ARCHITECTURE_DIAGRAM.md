# Architecture Diagram

## Dialer Pipeline

```mermaid
flowchart LR
  C[Campaign: borrowers + mode] --> P[Pacing Engine<br/>Progressive | Predictive]
  P -- "PacingRequest(n, why)<br/>just data" --> S[Safety Controller<br/>APPROVE / REDUCE / REJECT / FALLBACK]
  S -- "ApprovedDispatch<br/>(unforgeable token)" --> A[Call Allocator<br/>atomic reserve]
  A --> PP[Provider Pool<br/>circuit breakers, idempotent dial]
  PP --> TA[Provider A: fast, reliable]
  PP --> TB[Provider B: slow, dup/out-of-order events]
  TA & TB -- events, at-least-once --> W[Dialer worker: apply_event]
  A & W <--> DB[(State store<br/>agents / calls / borrowers / events)]
  P -. reads snapshot .-> DB
  S -. reads abandon stats + provider health .-> DB
```

### Component Roles & The Safety Boundary

1. **Campaign**: Holds borrower list and chosen mode (`PROGRESSIVE` or `PREDICTIVE`).
2. **Pacing Engine** (`smartdialer/pacing.py`):
   - Computes desired calls and returns only a frozen `PacingRequest(mode, requested, why)`.
   - Has **no reference** to the allocator or telecom providers.
3. **Safety Controller** (`smartdialer/safety.py`):
   - Sits strictly between Pacing Engine and Call Allocator.
   - Evaluates system health, abandon rate budget ($\le 3\%$), provider circuit breakers, and hard in-flight call caps.
   - Decides: `APPROVE`, `REDUCE`, `REJECT`, or `FALLBACK_PROGRESSIVE`.
   - Only component that can mint an `ApprovedDispatch` carrying a private capability token.
4. **Call Allocator** (`smartdialer/allocator.py`):
   - Re-checks capability token (forged tokens raise `PermissionError`).
   - Atomically reserves borrower and (if progressive) agent in a single database transaction.
5. **Provider Pool** (`smartdialer/providers.py`):
   - Wraps Provider A (fast/reliable) and Provider B (flaky, duplicate/out-of-order events).
   - Manages circuit breakers and ensures idempotent dials on `client_ref`.
6. **State Store** (`smartdialer/store.py`):
   - Single source of truth. Enforces atomic Compare-And-Swap (CAS) state changes and unique database constraints.
