"""State store: all agent/call/borrower transitions are single atomic transactions.

SQLite (BEGIN IMMEDIATE => one writer at a time) stands in for PostgreSQL. The same design maps
1:1 to Postgres: conditional UPDATE ... WHERE state=... and FOR UPDATE SKIP LOCKED for claims.
"""
import contextlib
import itertools
import json
import math
import sqlite3
import threading
from dataclasses import dataclass, field

from .models import (RANK, TERMINAL_SQL, AgentState as A, CallState as C, Policy, PROGRESSIVE)

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS agents(
  id TEXT PRIMARY KEY, state TEXT NOT NULL, call_id TEXT, idle_since REAL DEFAULT 0,
  wrap_until REAL, pending_state TEXT, version INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS ix_agents_state ON agents(state, idle_since);
CREATE TABLE IF NOT EXISTS borrowers(
  id TEXT PRIMARY KEY, phone TEXT, state TEXT NOT NULL DEFAULT 'READY', priority INTEGER DEFAULT 0,
  attempts INTEGER DEFAULT 0, dial_seq INTEGER DEFAULT 0, next_attempt_at REAL DEFAULT 0);
CREATE INDEX IF NOT EXISTS ix_borrowers_ready ON borrowers(state, priority DESC, next_attempt_at);
CREATE TABLE IF NOT EXISTS calls(
  id TEXT PRIMARY KEY, idem_key TEXT UNIQUE NOT NULL, borrower_id TEXT NOT NULL, agent_id TEXT,
  mode TEXT, provider TEXT, state TEXT NOT NULL, created_at REAL, updated_at REAL,
  answered_at REAL, ended_at REAL, reason TEXT);
CREATE INDEX IF NOT EXISTS ix_calls_state ON calls(state, updated_at);
CREATE INDEX IF NOT EXISTS ix_calls_answered ON calls(answered_at);
-- Hard DB-level guards: at most one live call per borrower and per agent, whatever the app code does.
CREATE UNIQUE INDEX IF NOT EXISTS ux_call_borrower_live ON calls(borrower_id) WHERE state NOT IN {TERMINAL_SQL};
CREATE UNIQUE INDEX IF NOT EXISTS ux_call_agent_live ON calls(agent_id) WHERE agent_id IS NOT NULL AND state NOT IN {TERMINAL_SQL};
CREATE TABLE IF NOT EXISTS provider_events(
  provider TEXT, event_id TEXT, call_id TEXT, type TEXT, outcome TEXT, PRIMARY KEY(provider, event_id));
CREATE TABLE IF NOT EXISTS incidents(ts REAL, kind TEXT, call_id TEXT);
CREATE TABLE IF NOT EXISTS decisions(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, mode TEXT, requested INT, approved INT,
  action TEXT, reason TEXT, detail TEXT);
"""


@dataclass
class Snapshot:
    now: float
    available: int = 0
    reserved: int = 0
    dialing: int = 0
    connected: int = 0
    wrap_up: int = 0
    paused: int = 0
    offline: int = 0
    unbound_inflight: int = 0   # predictive calls dialled with no agent attached yet
    bound_inflight: int = 0     # progressive calls (agent already reserved)
    free_soon: int = 0          # WRAP_UP agents that finish within the horizon
    ready_borrowers: int = 0


@dataclass
class Stats:
    outcomes: list = field(default_factory=list)  # newest first; 1=answered 0=no answer
    answered_w: int = 0
    abandoned_w: int = 0


class Store:
    def __init__(self, path=":memory:"):
        self.path = path
        self._mem = path == ":memory:"
        self._local = threading.local()
        self._memlock = threading.RLock()
        self._memconn = None
        self._ids = itertools.count(1)
        self._conn().executescript(SCHEMA)

    # ---- plumbing -------------------------------------------------------
    def _connect(self, path):
        c = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=120)
        c.row_factory = sqlite3.Row
        if path != ":memory:":
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
        return c

    def _conn(self):
        if self._mem:
            if self._memconn is None:
                self._memconn = self._connect(":memory:")
            return self._memconn
        c = getattr(self._local, "c", None)
        if c is None:
            c = self._local.c = self._connect(self.path)
        return c

    @contextlib.contextmanager
    def tx(self):
        c = self._conn()
        if self._mem:
            self._memlock.acquire()
        try:
            c.execute("BEGIN IMMEDIATE")
            try:
                yield c
            except BaseException:
                c.execute("ROLLBACK")
                raise
            else:
                c.execute("COMMIT")
        finally:
            if self._mem:
                self._memlock.release()

    def q(self, sql, args=()):
        return self._conn().execute(sql, args).fetchall()

    def one(self, sql, args=()):
        r = self._conn().execute(sql, args).fetchone()
        return r[0] if r is not None and len(r) == 1 else r

    # ---- setup ----------------------------------------------------------
    def add_agents(self, ids, state=A.OFFLINE):
        with self.tx() as c:
            c.executemany("INSERT OR IGNORE INTO agents(id,state) VALUES(?,?)", [(i, state) for i in ids])

    def add_borrowers(self, rows):
        with self.tx() as c:
            c.executemany("INSERT OR IGNORE INTO borrowers(id,phone) VALUES(?,?)", rows)

    def agent_login(self, agent_id, now):
        with self.tx() as c:
            c.execute("UPDATE agents SET state='AVAILABLE', idle_since=?, version=version+1 "
                      "WHERE id=? AND state IN ('OFFLINE','PAUSED')", (now, agent_id))

    # ---- agent leaves (logout / pause / network drop) ------------------------
    def agent_leave(self, agent_id, target, now):
        """Returns list of call ids the caller should cancel at the provider."""
        assert target in (A.OFFLINE, A.PAUSED)
        with self.tx() as c:
            a = c.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
            if a is None:
                return []
            if a["state"] in (A.AVAILABLE, A.WRAP_UP, A.OFFLINE, A.PAUSED):
                c.execute("UPDATE agents SET state=?, pending_state=NULL, wrap_until=NULL, version=version+1 "
                          "WHERE id=?", (target, agent_id))
                return []
            if a["state"] == A.CONNECTED:       # never drop a live conversation; leave after wrap-up
                c.execute("UPDATE agents SET pending_state=? WHERE id=?", (target, agent_id))
                return []
            # RESERVED / DIALING: call not yet answered -> cancel it, give the borrower back
            c.execute("UPDATE agents SET pending_state=? WHERE id=?", (target, agent_id))
            self._finish(c, a["call_id"], C.CANCELLED, "AGENT_LEFT", now, "requeue", A.AVAILABLE)
            return [a["call_id"]]

    def release_wrapups(self, now):
        with self.tx() as c:
            c.execute("UPDATE agents SET state=pending_state, pending_state=NULL, wrap_until=NULL, "
                      "version=version+1 WHERE state='WRAP_UP' AND wrap_until<=? AND pending_state IS NOT NULL", (now,))
            c.execute("UPDATE agents SET state='AVAILABLE', idle_since=wrap_until, wrap_until=NULL, "
                      "version=version+1 WHERE state='WRAP_UP' AND wrap_until<=?", (now,))

    # ---- read models ----------------------------------------------------
    def snapshot(self, now, horizon):
        s = Snapshot(now=now)
        for r in self.q("SELECT state, COUNT(*) n FROM agents GROUP BY state"):
            setattr(s, {"AVAILABLE": "available", "RESERVED": "reserved", "DIALING": "dialing",
                        "CONNECTED": "connected", "WRAP_UP": "wrap_up", "PAUSED": "paused",
                        "OFFLINE": "offline"}[r["state"]], r["n"])
        s.unbound_inflight = self.one("SELECT COUNT(*) FROM calls WHERE agent_id IS NULL AND state IN "
                                      "('RESERVED','INITIATED','RINGING')")
        s.bound_inflight = self.one("SELECT COUNT(*) FROM calls WHERE agent_id IS NOT NULL AND state IN "
                                    "('RESERVED','INITIATED','RINGING')")
        s.free_soon = self.one("SELECT COUNT(*) FROM agents WHERE state='WRAP_UP' AND pending_state IS NULL "
                               "AND wrap_until<=?", (now + horizon,))
        s.ready_borrowers = self.one("SELECT COUNT(*) FROM borrowers WHERE state='READY' AND next_attempt_at<=?", (now,))
        return s

    def stats(self, now, policy, k=150):
        rows = self.q("SELECT CASE WHEN answered_at IS NOT NULL THEN 1 ELSE 0 END a FROM calls "
                      "WHERE answered_at IS NOT NULL OR reason='NO_ANSWER' ORDER BY updated_at DESC LIMIT ?", (k,))
        lo = now - policy.abandon_window
        orphans = self.one("SELECT COUNT(*) FROM incidents WHERE kind='ORPHAN_ANSWER' AND ts>=?", (lo,))
        answered = self.one("SELECT COUNT(*) FROM calls WHERE answered_at>=?", (lo,)) + orphans
        abandoned = self.one("SELECT COUNT(*) FROM calls WHERE state='ABANDONED' AND ended_at>=?", (lo,)) + orphans
        return Stats([r["a"] for r in rows], answered, abandoned)

    def log_decision(self, now, mode, requested, approved, action, reason, detail):
        with self.tx() as c:
            c.execute("INSERT INTO decisions(ts,mode,requested,approved,action,reason,detail) VALUES(?,?,?,?,?,?,?)",
                      (now, mode, requested, approved, action, reason, json.dumps(detail)))

    # ---- allocation (borrower + agent reservation, one transaction) -----------
    def allocate(self, n, mode, now, policy: Policy):
        """Atomically reserve up to n (borrower[, agent]) pairs. Returns [(call_id, phone)].

        Progressive: bounded by the number of AVAILABLE agent rows seen *inside* the transaction, so
        concurrent workers can never reserve more calls than agents. Predictive: the in-flight cap is
        re-checked inside the transaction (check-and-reserve), not just at decision time.
        """
        out = []
        with self.tx() as c:
            agents = []
            if mode == PROGRESSIVE:
                agents = [r["id"] for r in c.execute(
                    "SELECT id FROM agents WHERE state='AVAILABLE' ORDER BY idle_since LIMIT ?", (n,))]
                n = len(agents)
            else:
                avail = c.execute("SELECT COUNT(*) FROM agents WHERE state='AVAILABLE'").fetchone()[0]
                soon = c.execute("SELECT COUNT(*) FROM agents WHERE state='WRAP_UP' AND pending_state IS NULL "
                                 "AND wrap_until<=?", (now + policy.horizon,)).fetchone()[0]
                unbound = c.execute("SELECT COUNT(*) FROM calls WHERE agent_id IS NULL AND state IN "
                                    "('RESERVED','INITIATED','RINGING')").fetchone()[0]
                cap = math.ceil(policy.max_ratio * (avail + soon))
                n = max(0, min(n, cap - unbound, policy.max_burst))
            if n <= 0:
                return out
            bs = c.execute("SELECT id, phone, dial_seq FROM borrowers WHERE state='READY' AND next_attempt_at<=? "
                           "ORDER BY priority DESC, next_attempt_at LIMIT ?", (now, n)).fetchall()
            for i, b in enumerate(bs):
                cid = f"c{next(self._ids)}-{b['id']}-{b['dial_seq'] + 1}"
                agent = agents[i] if mode == PROGRESSIVE else None
                c.execute("INSERT INTO calls(id,idem_key,borrower_id,agent_id,mode,state,created_at,updated_at) "
                          "VALUES(?,?,?,?,?,?,?,?)",
                          (cid, f"{b['id']}:{b['dial_seq'] + 1}", b["id"], agent, mode, C.RESERVED, now, now))
                c.execute("UPDATE borrowers SET state='IN_CALL', dial_seq=dial_seq+1 WHERE id=? AND state='READY'",
                          (b["id"],))
                if agent:
                    r = c.execute("UPDATE agents SET state='RESERVED', call_id=?, version=version+1 "
                                  "WHERE id=? AND state='AVAILABLE'", (cid, agent))
                    if r.rowcount != 1:      # compare-and-swap lost -> roll the whole batch back
                        raise RuntimeError("agent CAS failed")
                out.append((cid, b["phone"]))
        return out

    def mark_initiated(self, call_id, provider, now):
        with self.tx() as c:
            r = c.execute("UPDATE calls SET state='INITIATED', provider=?, updated_at=? WHERE id=? AND state='RESERVED'",
                          (provider, now, call_id))
            if r.rowcount:
                c.execute("UPDATE agents SET state='DIALING', version=version+1 WHERE call_id=? AND state='RESERVED'",
                          (call_id,))
            else:  # events may have outrun us; still remember which provider owns the call
                c.execute("UPDATE calls SET provider=COALESCE(provider,?) WHERE id=?", (provider, call_id))
            return bool(r.rowcount)

    def dial_failed(self, call_id, reason, now):
        """Provider never accepted the call: nothing reached the borrower, so no attempt is consumed."""
        with self.tx() as c:
            row = c.execute("SELECT state FROM calls WHERE id=?", (call_id,)).fetchone()
            if row and row["state"] in (C.RESERVED, C.INITIATED):
                self._finish(c, call_id, C.FAILED, reason, now, "requeue_backoff", A.AVAILABLE)

    # ---- provider events (idempotent, order-insensitive) ----------------------
    def apply_event(self, provider, event_id, call_id, etype, reason, now):
        with self.tx() as c:
            r = c.execute("INSERT OR IGNORE INTO provider_events(provider,event_id,call_id,type) VALUES(?,?,?,?)",
                          (provider, event_id, call_id, etype))
            if r.rowcount == 0:
                c.execute("INSERT INTO incidents VALUES(?,?,?)", (now, "DUPLICATE_EVENT", call_id))
                return "DUPLICATE"
            out = self._transition(c, call_id, etype, reason, now)
            c.execute("UPDATE provider_events SET outcome=? WHERE provider=? AND event_id=?", (out, provider, event_id))
            return out

    def _transition(self, c, call_id, etype, reason, now):
        call = c.execute("SELECT * FROM calls WHERE id=?", (call_id,)).fetchone()
        if call is None:
            return "UNKNOWN_CALL"
        st = call["state"]
        if st in (C.COMPLETED, C.FAILED, C.CANCELLED, C.ABANDONED):
            if etype == "ANSWERED" and st in (C.FAILED, C.CANCELLED):
                c.execute("INSERT INTO incidents VALUES(?,?,?)", (now, "ORPHAN_ANSWER", call_id))
                return "ORPHAN_ANSWER"     # a human picked up a call we already wrote off -> hang up
            return "IGNORED_TERMINAL"
        rank = RANK[st]
        if etype == "RINGING":
            if rank >= RANK[C.RINGING]:
                return "IGNORED_STALE"
            c.execute("UPDATE calls SET state='RINGING', updated_at=? WHERE id=?", (now, call_id))
            c.execute("UPDATE agents SET state='DIALING' WHERE call_id=? AND state='RESERVED'", (call_id,))
            return "APPLIED"
        if etype == "ANSWERED":
            if rank >= RANK[C.ANSWERED]:
                return "IGNORED_STALE"
            agent = call["agent_id"] or self._bind_agent(c, call_id)
            c.execute("UPDATE calls SET answered_at=?, updated_at=? WHERE id=?", (now, now, call_id))
            if agent is None:
                self._finish(c, call_id, C.ABANDONED, "NO_AGENT", now, "callback", A.AVAILABLE)
                return "ABANDONED"
            c.execute("UPDATE calls SET state='CONNECTED', agent_id=? WHERE id=?", (agent, call_id))
            c.execute("UPDATE agents SET state='CONNECTED', call_id=?, version=version+1 WHERE id=?", (call_id, agent))
            return "APPLIED"
        if etype == "COMPLETED":
            if st == C.CONNECTED:
                self._finish(c, call_id, C.COMPLETED, None, now, "done", A.WRAP_UP)
                return "APPLIED"
            # COMPLETED overtook ANSWERED/RINGING: accept the terminal fact; later stragglers are ignored.
            self._finish(c, call_id, C.COMPLETED, "OUT_OF_ORDER", now, "done", A.AVAILABLE)
            return "APPLIED_OUT_OF_ORDER"
        if etype == "FAILED":
            mid_call = st == C.CONNECTED
            self._finish(c, call_id, C.FAILED, reason or "FAILED", now, "retry", A.WRAP_UP if mid_call else A.AVAILABLE)
            return "APPLIED"
        return "IGNORED_UNKNOWN_TYPE"

    def _bind_agent(self, c, call_id):
        """Predictive: attach the longest-idle AVAILABLE agent at answer time (atomic claim)."""
        r = c.execute("SELECT id FROM agents WHERE state='AVAILABLE' ORDER BY idle_since LIMIT 1").fetchone()
        if r is None:
            return None
        u = c.execute("UPDATE agents SET state='RESERVED', call_id=? WHERE id=? AND state='AVAILABLE'", (call_id, r["id"]))
        return r["id"] if u.rowcount == 1 else None

    # ---- shared terminal handling ---------------------------------------
    def _finish(self, c, call_id, state, reason, now, borrower, agent_to):
        call = c.execute("SELECT * FROM calls WHERE id=?", (call_id,)).fetchone()
        n = c.execute(f"UPDATE calls SET state=?, ended_at=?, updated_at=?, reason=COALESCE(?,reason) "
                      f"WHERE id=? AND state NOT IN {TERMINAL_SQL}", (state, now, now, reason, call_id)).rowcount
        if n == 0:
            return False
        bid = call["borrower_id"]
        if borrower == "done":
            c.execute("UPDATE borrowers SET state='DONE', attempts=attempts+1 WHERE id=?", (bid,))
        elif borrower == "retry":
            att = c.execute("SELECT attempts FROM borrowers WHERE id=?", (bid,)).fetchone()[0] + 1
            if att >= self._max_attempts:
                c.execute("UPDATE borrowers SET state='DONE', attempts=? WHERE id=?", (att, bid))
            else:
                c.execute("UPDATE borrowers SET state='READY', attempts=?, next_attempt_at=? WHERE id=?",
                          (att, now + self._backoff * 2 ** (att - 1), bid))
        elif borrower == "requeue_backoff":
            c.execute("UPDATE borrowers SET state='READY', next_attempt_at=? WHERE id=?", (now + self._backoff / 3, bid))
        elif borrower == "requeue":
            c.execute("UPDATE borrowers SET state='READY', next_attempt_at=? WHERE id=?", (now, bid))
        elif borrower == "callback":   # abandoned: call back quickly, with priority
            c.execute("UPDATE borrowers SET state='READY', priority=10, next_attempt_at=? WHERE id=?", (now + 5, bid))
        # Agent release is fenced by call_id: never stomp on an agent that already moved to another call.
        agent_id = c.execute("SELECT agent_id FROM calls WHERE id=?", (call_id,)).fetchone()[0]
        if agent_id:
            a = c.execute("SELECT pending_state FROM agents WHERE id=? AND call_id=?", (agent_id, call_id)).fetchone()
            if a is not None:
                if agent_to == A.WRAP_UP:
                    c.execute("UPDATE agents SET state='WRAP_UP', call_id=NULL, wrap_until=?, version=version+1 "
                              "WHERE id=?", (now + self._wrap, agent_id))
                elif a["pending_state"]:
                    c.execute("UPDATE agents SET state=pending_state, pending_state=NULL, call_id=NULL, "
                              "version=version+1 WHERE id=?", (agent_id,))
                else:
                    c.execute("UPDATE agents SET state='AVAILABLE', call_id=NULL, idle_since=?, version=version+1 "
                              "WHERE id=?", (now, agent_id))
        return True

    # policy values used by _finish; set by Dialer/tests via configure()
    _max_attempts, _backoff, _wrap = 3, 30.0, 10.0

    def configure(self, policy: Policy):
        self._max_attempts, self._backoff, self._wrap = policy.max_attempts, policy.retry_backoff, policy.wrap_seconds
        return self

    # ---- crash recovery / reconciliation ----------------------------------
    def sweep(self, now, policy: Policy):
        """Repairs state left behind by crashed workers / lost events. Safe to run from any worker."""
        redrive, cancel, hangup = [], [], []
        with self.tx() as c:
            for r in c.execute("SELECT c.id, b.phone FROM calls c JOIN borrowers b ON b.id=c.borrower_id "
                               "WHERE c.state='RESERVED' AND c.updated_at<=?", (now - policy.reserve_timeout,)).fetchall():
                redrive.append((r["id"], r["phone"]))
            for r in c.execute("SELECT id FROM calls WHERE state IN ('INITIATED','RINGING') AND updated_at<=?",
                               (now - policy.setup_timeout,)).fetchall():
                if self._finish(c, r["id"], C.FAILED, "SETUP_TIMEOUT", now, "retry", A.AVAILABLE):
                    cancel.append(r["id"])
            for r in c.execute("SELECT id FROM calls WHERE state IN ('ANSWERED','CONNECTED') AND updated_at<=?",
                               (now - policy.max_call_seconds,)).fetchall():
                if self._finish(c, r["id"], C.FAILED, "MAX_DURATION", now, "done", A.WRAP_UP):
                    hangup.append(r["id"])
        return redrive, cancel, hangup

    # ---- invariants (used by tests and simulator) -----------------------------
    def check_invariants(self):
        errs = []
        q = self.one
        if q(f"SELECT COUNT(*) FROM (SELECT agent_id FROM calls WHERE agent_id IS NOT NULL AND state NOT IN {TERMINAL_SQL} "
             f"GROUP BY agent_id HAVING COUNT(*)>1)"):
            errs.append("agent has >1 live call")
        if q(f"SELECT COUNT(*) FROM (SELECT borrower_id FROM calls WHERE state NOT IN {TERMINAL_SQL} "
             f"GROUP BY borrower_id HAVING COUNT(*)>1)"):
            errs.append("borrower has >1 live call")
        if q(f"SELECT COUNT(*) FROM agents a WHERE a.state IN ('RESERVED','DIALING','CONNECTED') AND NOT EXISTS "
             f"(SELECT 1 FROM calls c WHERE c.id=a.call_id AND c.state NOT IN {TERMINAL_SQL})"):
            errs.append("busy agent without live call")
        if q(f"SELECT COUNT(*) FROM calls c WHERE c.agent_id IS NOT NULL AND c.state NOT IN {TERMINAL_SQL} AND NOT EXISTS "
             f"(SELECT 1 FROM agents a WHERE a.call_id=c.id)"):
            errs.append("live call whose agent does not point back at it")
        if q(f"SELECT COUNT(*) FROM borrowers b WHERE b.state='IN_CALL' AND NOT EXISTS "
             f"(SELECT 1 FROM calls c WHERE c.borrower_id=b.id AND c.state NOT IN {TERMINAL_SQL})"):
            errs.append("IN_CALL borrower without live call")
        return errs
