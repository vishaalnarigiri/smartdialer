"""Deterministic discrete-time simulator.  python -m smartdialer.sim [--quick]"""
import argparse
import random
import sys
import time

from .engine import Dialer
from .models import Policy, PROGRESSIVE
from .pacing import PredictivePacer, ProgressivePacer
from .providers import ProviderPool, World, provider_a, provider_b
from .safety import SafetyController
from .store import Store

SCENARIOS = {
    "A (20%, 120s)": (lambda t: 0.20, lambda t: 120),
    "B (50%, 90s)": (lambda t: 0.50, lambda t: 90),
    "C (70%, 180s)": (lambda t: 0.70, lambda t: 180),
    # D: answer rate swings 60% -> 15% -> 70%, talk time 60s -> 180s -> 90s
    "D (changing)": (lambda t: 0.60 if t < 600 else 0.15 if t < 1200 else 0.70,
                     lambda t: 60 if t < 600 else 180 if t < 1200 else 90),
}


def run(scn, mode, agents=100, duration=1800, dt=1.0, seed=7, providers="AB", outages=(), drop=None,
        policy=None, borrowers=60000):
    ans, talk = SCENARIOS[scn]
    rng = random.Random(seed)
    world = World(ans, lambda t, r: talk(t) * r.uniform(0.5, 1.5), rng)
    provs = {"A": provider_a(world, seed + 1), "B": provider_b(world, seed + 2)}
    for name, s, e in outages:
        provs[name].add_outage(s, e)
    pool = ProviderPool([provs[c] for c in providers], seed=seed + 3)
    policy = policy or Policy()
    store = Store().configure(policy)
    ids = [f"ag{i}" for i in range(agents)]
    store.add_agents(ids)
    for a in ids:
        store.agent_login(a, 0)
    store.add_borrowers([(f"b{i}", f"+91{9000000000 + i}") for i in range(borrowers)])
    pacer = ProgressivePacer() if mode == PROGRESSIVE else PredictivePacer()
    d = Dialer(store, pool, pacer, SafetyController(policy), policy)
    busy = online = 0
    req_sum = app_sum = 0
    reaction = {}
    for step in range(int(duration / dt)):
        t = step * dt
        if drop and drop[0] <= t < drop[0] + drop[2]:          # agents vanish over drop[2] seconds
            k = round(drop[1] / drop[2] * dt)
            for a in [x for x in ids if store.one("SELECT state FROM agents WHERE id=?", (x,)) != "OFFLINE"][:k]:
                for cid in store.agent_leave(a, "OFFLINE", t):
                    pool.cancel(None, cid, t)
        d.pump_events(t)
        req, dec = d.tick(t)
        req_sum += dec.requested
        app_sum += dec.approved
        r = store.one("SELECT SUM(state='CONNECTED'), SUM(state NOT IN ('OFFLINE','PAUSED')) FROM agents")
        busy += r[0] or 0
        online += r[1] or 0
        if step % 100 == 0:
            errs = store.check_invariants()
            assert not errs, (t, errs)
    errs = store.check_invariants()
    assert not errs, errs
    n = lambda sql: store.one(sql)
    answered = n("SELECT COUNT(*) FROM calls WHERE answered_at IS NOT NULL") + n("SELECT COUNT(*) FROM incidents WHERE kind='ORPHAN_ANSWER'")
    abandoned = n("SELECT COUNT(*) FROM calls WHERE state='ABANDONED'") + n("SELECT COUNT(*) FROM incidents WHERE kind='ORPHAN_ANSWER'")
    acts = {r[0]: r[1] for r in store.q("SELECT action, COUNT(*) FROM decisions GROUP BY action")}
    res = dict(scn=scn, mode=mode[:4], util=busy / max(online, 1), dialed=n("SELECT COUNT(*) FROM calls WHERE state!='RESERVED' OR provider IS NOT NULL"),
               answered=answered, connected=n("SELECT COUNT(*) FROM calls WHERE agent_id IS NOT NULL AND answered_at IS NOT NULL"),
               abandoned=abandoned, abandon=abandoned / max(answered, 1), req=req_sum / (duration / dt), app=app_sum / (duration / dt),
               acts=acts, perr=n("SELECT COUNT(*) FROM calls WHERE reason='PROVIDER_ERROR'"),
               setup_to=n("SELECT COUNT(*) FROM calls WHERE reason='SETUP_TIMEOUT'"),
               dups=n("SELECT COUNT(*) FROM incidents WHERE kind='DUPLICATE_EVENT'"),
               ooo=n("SELECT COUNT(*) FROM provider_events WHERE outcome LIKE 'IGNORED%' OR outcome LIKE 'APPLIED_OUT%'"),
               orphans=n("SELECT COUNT(*) FROM incidents WHERE kind='ORPHAN_ANSWER'"), store=store, provs=provs)
    if drop:
        lo, hi = drop[0], drop[0] + 90
        res["aband_after_drop"] = n(f"SELECT COUNT(*) FROM calls WHERE state='ABANDONED' AND ended_at BETWEEN {lo} AND {hi}")
        res["aband_before_drop"] = n(f"SELECT COUNT(*) FROM calls WHERE state='ABANDONED' AND ended_at < {lo}")
    return res


def row(r):
    a = r["acts"]
    return (f"| {r['scn']} | {r['mode']} | {r['util']:.0%} | {r['dialed']} | {r['answered']} | {r['connected']} | "
            f"{r['abandoned']} | {r['abandon']:.2%} | {r['req']:.1f} / {r['app']:.1f} | "
            f"{a.get('APPROVE', 0)}/{a.get('REDUCE', 0)}/{a.get('REJECT', 0)}/{a.get('FALLBACK_PROGRESSIVE', 0)} (idle {a.get('IDLE', 0)}) |")


HEAD = ("| Scenario | Mode | Agent talk utilisation | Calls dialled | Answered | Connected | Abandoned | Abandon rate | "
        "Pacing req / approved per tick | Safety APPROVE/REDUCE/REJECT/FALLBACK |\n|---|---|---|---|---|---|---|---|---|---|")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--out", default="results/simulation.md")
    a = ap.parse_args()
    dur, ag = (600, 50) if a.quick else (1800, 100)
    out = [f"# Simulation results ({ag} agents, {dur}s virtual time, seed 7)\n", "## 1. Scenarios, mixed providers A(60%)+B(40%)\n", HEAD]
    t0 = time.time()
    for scn in SCENARIOS:
        for mode in ("PROGRESSIVE", "PREDICTIVE"):
            out.append(row(run(scn, mode, agents=ag, duration=dur)))
            print(out[-1]); sys.stdout.flush()
    out += ["", "## 2. Provider failure (scenario B, predictive)\n", HEAD]
    cases = {
        "B + provider A hard outage 300-420s": dict(outages=[("A", 300, 420)]),
        "B + BOTH providers down 300-360s": dict(outages=[("A", 300, 360), ("B", 300, 360)]),
        "B + only flaky provider B": dict(providers="B"),
    }
    for label, kw in cases.items():
        r = run("B (50%, 90s)", "PREDICTIVE", agents=ag, duration=dur, **kw)
        r["scn"] = label
        out.append(row(r) + f" provider-errors={r['perr']} setup-timeouts={r['setup_to']} dup-events-ignored={r['dups']} stale/out-of-order-handled={r['ooo']} orphan-answers-hung-up={r['orphans']}")
        print(out[-1]); sys.stdout.flush()
    out += ["", "## 3. Agent availability drop (100 -> 60 agents within 5s at t=300s, scenario B, predictive)\n"]
    r = run("B (50%, 90s)", "PREDICTIVE", agents=ag, duration=dur, drop=(300, int(ag * 0.4), 5))
    out.append(f"- abandoned calls in the 90s after the drop: **{r['aband_after_drop']}** (before the drop: {r['aband_before_drop']}); "
               f"overall abandon rate {r['abandon']:.2%}; safety actions APPROVE/REDUCE/REJECT/FALLBACK = "
               f"{r['acts'].get('APPROVE',0)}/{r['acts'].get('REDUCE',0)}/{r['acts'].get('REJECT',0)}/{r['acts'].get('FALLBACK_PROGRESSIVE',0)}")
    out.append(f"\n_runtime {time.time() - t0:.0f}s_")
    import os
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    open(a.out, "w").write("\n".join(out) + "\n")
    print("\nwrote", a.out)


if __name__ == "__main__":
    main()
