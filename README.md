# SmartDialer prototype

Progressive **and** predictive dialing with a Safety Controller that the pacing logic cannot bypass.
Pure Python 3.10+ standard library (SQLite for state). **No installs needed.**

```
Campaign ─> Pacing Engine ─> Safety Controller ─> Call Allocator ─> Provider Pool ─> Telecom (A | B)
            (Progressive /     approve / reduce /    reserves borrower   circuit breakers,
             Predictive)        reject / fallback     (+agent) atomically  idempotent dial
                 ▲                                          │                    │
                 └──────────── State Store (agents, calls, borrowers) ◄── events ─┘
```

## Run it

```bash
python3 -m unittest discover -s tests -t . -v     # 29 tests (races, duplicates, out-of-order, crashes, outage, safety)
python3 -m smartdialer.sim --quick                # ~1 min  : 4 scenarios x 2 modes + failure cases
python3 -m smartdialer.sim                        # ~full   : 100 agents, 30 min virtual time -> results/simulation.md
python3 -m smartdialer.load_test                  # contention, event throughput, per-tick cost at 100/1k/10k agents
```

## Layout

| File | What it is |
|---|---|
| `smartdialer/models.py` | Agent + call states, forward-only ranks, the frozen/validated `Policy` |
| `smartdialer/store.py` | All state transitions as single atomic transactions; recovery sweep; invariant checker |
| `smartdialer/pacing.py` | `ProgressivePacer`, `PredictivePacer` — return only a `PacingRequest` (a number + explanation) |
| `smartdialer/safety.py` | `SafetyController` — the only thing that can mint an `ApprovedDispatch` |
| `smartdialer/allocator.py` | Verifies the dispatch, reserves, dials. Refuses anything not minted by Safety |
| `smartdialer/providers.py` | Provider interface, `provider_a` (fast/reliable), `provider_b` (slow, timeouts, dup + out-of-order events), pool + circuit breakers |
| `smartdialer/engine.py` | A dialer worker (`tick`, `pump_events`, `recover`). Run N of them against one store |
| `smartdialer/sim.py`, `load_test.py` | Simulator and load test |
| `docs/ARCHITECTURE.md` | Diagrams, state machines, decisions, failure walk-throughs, scale analysis, final answer |
| `results/simulation.md` | Output of the full simulation |

## Submission Checklist

Every requested item from the specification is implemented and verifiable:

| Required Item | Location / Implementation | Verification Command |
|---|---|---|
| **Working source code** | `smartdialer/` (pure Python stdlib, 0 external dependencies) | `python3 -m unittest discover -s tests -t .` |
| **README with setup** | `README.md` (no external installs, Python 3.10+ stdlib) | Just clone and run |
| **Architecture diagram** | `docs/ARCHITECTURE_DIAGRAM.md` (also in `docs/ARCHITECTURE.md` §1) | Rendered on GitHub |
| **Agent state machine** | `docs/AGENT_STATE_MACHINE.md` (also in `docs/ARCHITECTURE.md` §2) | 7 states + CAS transitions |
| **Call state machine** | `docs/CALL_STATE_MACHINE.md` (also in `docs/ARCHITECTURE.md` §2) | Rank-monotonic forward-only |
| **Progressive Dialer** | `smartdialer/pacing.py` (`ProgressivePacer`) | `TestSafetyAndPacing` |
| **Predictive Pacing Engine** | `smartdialer/pacing.py` (`PredictivePacer`) | Mathematical Beta-posterior pacing |
| **Safety Controller** | `smartdialer/safety.py` (`SafetyController`) | Unbypassable dispatch token |
| **Mock telecom providers** | `smartdialer/providers.py` (Provider A & B, circuit breakers) | `TestProviders` |
| **Tests** | `tests/test_dialer.py` (29 comprehensive unit & concurrency tests) | `python3 -m unittest discover -s tests -t . -v` |
| **Basic simulation** | `smartdialer/sim.py` (Scenarios A–D + Outages) | `python3 -m smartdialer.sim --quick` |
| **Basic load test** | `smartdialer/load_test.py` (Contention, throughput, scale latency) | `python3 -m smartdialer.load_test` |
| **Architecture decision doc** | `docs/ARCHITECTURE_DECISION.md` (also in `docs/ARCHITECTURE.md`) | Full analysis & tradeoffs |

## Not done (deliberately, in the timebox)
No real telecom integration (the `TelecomProvider` interface is what a Plivo adapter would implement), no UI/dashboard,
no Postgres adapter (the SQL is written as conditional UPDATEs so it ports directly), no ML.
