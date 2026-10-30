# Simulation results (50 agents, 600s virtual time, seed 7)

## 1. Scenarios, mixed providers A(60%)+B(40%)

| Scenario | Mode | Agent talk utilisation | Calls dialled | Answered | Connected | Abandoned | Abandon rate | Pacing req / approved per tick | Safety APPROVE/REDUCE/REJECT/FALLBACK |
|---|---|---|---|---|---|---|---|---|---|
| A (20%, 120s) | PROG | 44% | 599 | 120 | 120 | 0 | 0.00% | 1.0 / 1.0 | 321/0/0/0 (idle 279) |
| A (20%, 120s) | PRED | 65% | 911 | 179 | 179 | 0 | 0.00% | 1.7 / 1.5 | 236/9/2/3 (idle 350) |
| B (50%, 90s) | PROG | 62% | 459 | 221 | 221 | 0 | 0.00% | 0.8 / 0.8 | 287/0/0/0 (idle 313) |
| B (50%, 90s) | PRED | 72% | 533 | 252 | 251 | 1 | 0.40% | 1.2 / 0.9 | 248/0/16/14 (idle 322) |
| C (70%, 180s) | PROG | 82% | 242 | 157 | 157 | 0 | 0.00% | 0.4 / 0.4 | 153/0/0/0 (idle 447) |
| C (70%, 180s) | PRED | 84% | 250 | 161 | 158 | 3 | 1.86% | 1.8 / 0.4 | 61/0/183/82 (idle 274) |
| D (changing) | PROG | 57% | 524 | 307 | 307 | 0 | 0.00% | 0.9 / 0.9 | 301/0/0/0 (idle 299) |
| D (changing) | PRED | 68% | 620 | 368 | 368 | 0 | 0.00% | 1.1 / 1.0 | 302/0/0/2 (idle 296) |

## 2. Provider failure (scenario B, predictive)

| Scenario | Mode | Agent talk utilisation | Calls dialled | Answered | Connected | Abandoned | Abandon rate | Pacing req / approved per tick | Safety APPROVE/REDUCE/REJECT/FALLBACK |
|---|---|---|---|---|---|---|---|---|---|
| B + provider A hard outage 300-420s | PRED | 68% | 487 | 230 | 225 | 5 | 2.17% | 2.8 / 0.8 | 142/0/132/104 (idle 222) | provider-errors=27 setup-timeouts=4 dup-events-ignored=91 stale/out-of-order-handled=4 orphan-answers-hung-up=4
| B + BOTH providers down 300-360s | PRED | 65% | 500 | 231 | 222 | 9 | 3.90% | 4.4 / 0.8 | 120/0/126/110 (idle 244) | provider-errors=33 setup-timeouts=10 dup-events-ignored=74 stale/out-of-order-handled=9 orphan-answers-hung-up=8
| B + only flaky provider B | PRED | 68% | 565 | 242 | 242 | 0 | 0.00% | 1.1 / 0.9 | 229/0/4/10 (idle 357) | provider-errors=47 setup-timeouts=0 dup-events-ignored=203 stale/out-of-order-handled=7 orphan-answers-hung-up=0

## 3. Agent availability drop (100 -> 60 agents within 5s at t=300s, scenario B, predictive)

- abandoned calls in the 90s after the drop: **0** (before the drop: 1); overall abandon rate 0.41%; safety actions APPROVE/REDUCE/REJECT/FALLBACK = 226/0/27/14

_runtime 33s_
