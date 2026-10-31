"""
PERSON 3 - RemoteWorkerManager : replaces Person 2's in-process WorkerManager for the distributed demo.

Person 2's WorkerManager runs worker *threads* inside the job-manager process and hands them jobs
through shared memory.  That cannot work when workers are separate Mininet hosts.  This class keeps
the same public shape (.scheduler, .start(), .stop()) and keeps using Person 2's WorkerScheduler and
Person 1's JobStore methods, but the workers are real programs (worker_node.py) that connect over TCP.

PROTOCOL  (newline-delimited JSON over TCP, job manager listens on WORKER_PORT=5002, workers connect)
  worker -> JM   {"type":"register","worker_id":"worker1","running":[{"job_id","attempt"}],"unsent_results":[...]}
  JM -> worker   {"type":"registered","worker_id":"worker1","heartbeat_interval":2.0}
  worker -> JM   {"type":"heartbeat","worker_id":"worker1","busy":true,"job_id":"ab12cd34"}   every 2 s
  JM -> worker   {"type":"heartbeat_ack"}
  JM -> worker   {"type":"job","job_id":"ab12cd34","attempt":1,"payload":{...,"job_id":"ab12cd34"}}
  worker -> JM   {"type":"result","worker_id":..,"job_id":..,"attempt":1,"status":"COMPLETED","result":..}
                 (or "status":"FAILED","error":"...")
  JM -> worker   {"type":"result_ack","job_id":..}

FAILURE HANDLING  (the point of this file)
  * TCP connection closes (worker program killed)  -> wait DISCONNECT_GRACE for it to come back,
                                                      then requeue its jobs                       [process failure]
  * heartbeats stop, controller says path is fine   -> requeue its jobs                           [process failure]
  * heartbeats stop / controller says "no path"     -> state UNREACHABLE: stop sending it work, KEEP its jobs
        - heartbeats resume (reroute worked)        -> back to normal, nothing re-run                [link failure]
        - still silent after NETWORK_GRACE seconds  -> requeue its jobs                              [partition]
  * Every result is checked: it is applied only if the job is still RUNNING on that worker with the
    same attempt number.  Late results from a worker we gave up on are discarded, so a client never
    receives two results for one job.   (Execution is at-least-once; result delivery is at-most-once.)
"""
import json
import os
import socket
import threading
import time
from collections import deque

import netconfig as cfg
from scheduler import WorkerScheduler          # Person 2 (unchanged)
from job_manager import RUNNING                # Person 1 (unchanged) - state name constant


def now_str():
    return time.strftime("%H:%M:%S")


class WorkerConn:
    """One TCP connection to one worker; serialises sends."""

    def __init__(self, sock, addr):
        self.sock, self.addr = sock, addr
        self._send_lock = threading.Lock()
        self.closed = False

    def send(self, obj):
        try:
            with self._send_lock:
                self.sock.sendall((json.dumps(obj) + "\n").encode())
            return True
        except OSError:
            self.close()
            return False

    def close(self):
        self.closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


class WorkerState:
    def __init__(self, wid, conn, ip):
        self.wid, self.conn, self.ip = wid, conn, ip
        self.state = "ONLINE"          # ONLINE | UNREACHABLE | DISCONNECTED | OFFLINE | REPLACED
        self.last_hb = time.time()
        self.jobs = {}                 # job_id -> attempt   (jobs the JM believes are RUNNING here)
        self.stale_jobs = set()        # jobs the worker is still busy with but JM no longer tracks
        self.since = time.time()       # when the current state began
        self.deadline = None           # for DISCONNECTED
        self.note = ""


class RemoteWorkerManager:
    def __init__(self, store, host="0.0.0.0", port=cfg.WORKER_PORT, sdn=None,
                 heartbeat_interval=cfg.HEARTBEAT_INTERVAL, heartbeat_timeout=cfg.HEARTBEAT_TIMEOUT,
                 disconnect_grace=cfg.DISCONNECT_GRACE, network_grace=cfg.NETWORK_GRACE,
                 topology_settle=cfg.TOPOLOGY_SETTLE, status_file=cfg.JM_STATUS_FILE):
        self.store, self.host, self.port, self.sdn = store, host, port, sdn
        self.hb_interval, self.hb_timeout = heartbeat_interval, heartbeat_timeout
        self.disconnect_grace, self.network_grace = disconnect_grace, network_grace
        self.topology_settle = topology_settle
        self.status_file = status_file
        self.scheduler = WorkerScheduler()        # Person 2's class, used for IDLE/BUSY bookkeeping
        self.workers = {}                         # worker_id -> WorkerState
        self.events = deque(maxlen=40)
        self._lock = threading.RLock()
        self.running = False
        self._srv = None

    # ---------------------------------------------------------------- small helpers
    def log(self, msg):
        print("[%s] [JM] %s" % (now_str(), msg), flush=True)
        self.events.append({"t": round(time.time(), 2), "msg": msg})

    def _set_sched(self, wid, status):
        """WorkerScheduler has no OFFLINE/UNREACHABLE helpers; its `workers` dict is plain, and
        get_available_worker() only ever returns entries equal to 'IDLE', so we just set the label."""
        if wid not in self.scheduler.workers:
            self.scheduler.register_worker(wid)
        self.scheduler.workers[wid] = status

    def _refresh_sched(self, ws):
        if ws.state == "ONLINE":
            self._set_sched(ws.wid, "BUSY" if (ws.jobs or ws.stale_jobs) else "IDLE")
        elif ws.state == "UNREACHABLE":
            self._set_sched(ws.wid, "UNREACHABLE")
        else:
            self._set_sched(ws.wid, "OFFLINE")

    # ---------------------------------------------------------------- lifecycle
    def start(self):
        self.running = True
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind((self.host, self.port))
        self._srv.listen(20)
        for fn in (self._accept_loop, self._dispatch_loop, self._monitor_loop):
            threading.Thread(target=fn, daemon=True).start()
        self.log("accepting WORKERS on %s:%d (heartbeat timeout %.0fs, network grace %.0fs)"
                 % (self.host, self.port, self.hb_timeout, self.network_grace))

    def stop(self):
        self.running = False
        try:
            self._srv.close()
        except (OSError, AttributeError):
            pass
        with self._lock:
            for ws in self.workers.values():
                if ws.conn:
                    ws.conn.close()

    # ---------------------------------------------------------------- networking
    def _accept_loop(self):
        while self.running:
            try:
                sock, addr = self._srv.accept()
            except OSError:
                break
            threading.Thread(target=self._conn_loop, args=(sock, addr), daemon=True).start()

    def _conn_loop(self, sock, addr):
        conn = WorkerConn(sock, addr)
        sock.settimeout(1.0)
        ws, buf, t0 = None, b"", time.time()
        try:
            while self.running and not conn.closed:
                try:
                    chunk = sock.recv(4096)
                except socket.timeout:
                    if ws is None and time.time() - t0 > 5:
                        break                      # never registered
                    continue
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if not line.strip():
                        continue
                    try:
                        msg = json.loads(line.decode())
                    except ValueError:
                        conn.send({"type": "error", "message": "invalid JSON"})
                        continue
                    ws = self._handle(msg, conn, ws, addr)
        except OSError:
            pass
        finally:
            self._conn_closed(conn, ws)
            conn.close()

    def _handle(self, msg, conn, ws, addr):
        t = msg.get("type")
        if t == "register":
            return self._on_register(msg, conn, addr)
        if ws is None:
            conn.send({"type": "error", "message": "register first"})
            return ws
        if t == "heartbeat":
            self._on_heartbeat(ws, conn)
        elif t == "result":
            self._on_result(ws, msg, conn)
        else:
            conn.send({"type": "error", "message": "unknown message type %r" % t})
        return ws

    # ---------------------------------------------------------------- registration / reconnection
    def _on_register(self, msg, conn, addr):
        wid = str(msg.get("worker_id", "")).strip()
        if not wid:
            conn.send({"type": "error", "message": "register needs worker_id"})
            return None
        with self._lock:
            old = self.workers.get(wid)
            ws = WorkerState(wid, conn, addr[0])
            if old is not None:
                prev_state = old.state
                if old.conn is not None and old.conn is not conn:
                    old.conn.close()
                old.conn, old.state = None, "REPLACED"
                self.log("worker %s RE-REGISTERED from %s (previous state: %s)" % (wid, addr[0], prev_state))
            else:
                self.log("worker %s REGISTERED from %s" % (wid, addr[0]))
            self.workers[wid] = ws

            # 1) results the worker could not deliver earlier
            for r in msg.get("unsent_results", []):
                self._apply_result(ws, r)
                conn.send({"type": "result_ack", "job_id": r.get("job_id")})

            # 2) reconcile: what the JM thinks is running here vs what the worker says it has
            reported = {j.get("job_id"): j.get("attempt") for j in msg.get("running", [])}
            for jid in self.store.running_jobs_of(wid):
                job = self.store.get(jid)
                if jid in reported and reported[jid] == job.attempts:
                    ws.jobs[jid] = job.attempts
                    self.log("worker %s still has job %s (attempt %d) -> keeping it, NOT re-running" % (wid, jid, job.attempts))
                else:
                    self._requeue(jid, wid, "worker %s reconnected without that job (it restarted/lost it)" % wid)
            for jid in reported:
                if jid not in ws.jobs:
                    ws.stale_jobs.add(jid)          # worker is busy with something we no longer track
            self.scheduler.register_worker(wid)
            self._refresh_sched(ws)
            conn.send({"type": "registered", "worker_id": wid, "heartbeat_interval": self.hb_interval})
        return ws

    def _on_heartbeat(self, ws, conn):
        with self._lock:
            if self.workers.get(ws.wid) is not ws:
                return
            ws.last_hb = time.time()
            if ws.state == "UNREACHABLE":
                ws.state, ws.since = "ONLINE", time.time()
                self.log("worker %s heartbeats RESUMED -> reachable again; its jobs %s are kept, nothing is re-run"
                         % (ws.wid, sorted(ws.jobs) or "(none)"))
                self._refresh_sched(ws)
        conn.send({"type": "heartbeat_ack", "t": time.time()})

    def _conn_closed(self, conn, ws):
        if ws is None:
            return
        with self._lock:
            if self.workers.get(ws.wid) is not ws or ws.state in ("OFFLINE", "REPLACED"):
                return
            reach = self.sdn.host_reachable(ws.ip) if self.sdn else None
            why = ("controller says path is UP -> the worker program exited, or it is reconnecting" if reach is True else
                   "controller says path is DOWN -> NETWORK problem" if reach is False else
                   "no controller data")
            ws.conn, ws.state, ws.since = None, "DISCONNECTED", time.time()
            ws.deadline = time.time() + self.disconnect_grace
            self._set_sched(ws.wid, "OFFLINE")
            self.log("worker %s TCP connection CLOSED (%s). Waiting %.0fs for it to return before requeueing %s"
                     % (ws.wid, why, self.disconnect_grace, sorted(ws.jobs) or "(no jobs)"))

    # ---------------------------------------------------------------- results
    def _on_result(self, ws, msg, conn):
        with self._lock:
            self._apply_result(ws, msg)
        conn.send({"type": "result_ack", "job_id": msg.get("job_id")})

    def _apply_result(self, ws, msg):
        """Apply a worker's result only if it is still valid.  Caller holds self._lock."""
        wid, jid, attempt = ws.wid, msg.get("job_id"), msg.get("attempt")
        job = self.store.get(jid)
        valid = (job is not None and job.state == RUNNING and job.worker_id == wid and job.attempts == attempt)
        if valid:
            if msg.get("status") == "COMPLETED":
                self.store.mark_completed(jid, msg.get("result"))
                self.log("job %s COMPLETED by %s (attempt %s) result=%s" % (jid, wid, attempt, msg.get("result")))
            else:
                self.store.mark_failed(jid, msg.get("error", "worker reported failure"))
                self.log("job %s FAILED on %s: %s" % (jid, wid, msg.get("error")))
        else:
            cur = (job.state, job.worker_id, job.attempts) if job else None
            self.log("DISCARDED stale result for job %s from %s attempt %s (job now: %s) - no duplicate delivered"
                     % (jid, wid, attempt, cur))
        ws.jobs.pop(jid, None)
        ws.stale_jobs.discard(jid)
        if self.workers.get(wid) is ws:
            self._refresh_sched(ws)

    def _requeue(self, jid, wid, reason):
        """Safe requeue: only if the job really is RUNNING on `wid`.  Caller holds self._lock."""
        job = self.store.get(jid)
        if job is None or job.state != RUNNING or job.worker_id != wid:
            return
        attempts = job.attempts
        if self.store.requeue(jid):
            self.log("job %s REQUEUED (attempt %d of %d used): %s" % (jid, attempts, self.store.max_attempts, reason))
        else:
            self.log("job %s FAILED permanently - retry limit (%d attempts) reached: %s" % (jid, attempts, reason))

    def _fail_worker(self, ws, reason):
        """Declare a worker lost and put its unfinished jobs back in the queue.  Holds self._lock."""
        ws.state, ws.since = "OFFLINE", time.time()
        if ws.conn:
            ws.conn.close()
            ws.conn = None
        self._set_sched(ws.wid, "OFFLINE")
        jobs = self.store.running_jobs_of(ws.wid)
        self.log("worker %s declared DEAD: %s. Unfinished jobs: %s" % (ws.wid, reason, jobs or "none"))
        for jid in jobs:
            self._requeue(jid, ws.wid, reason)
        ws.jobs.clear()
        ws.stale_jobs.clear()

    # ---------------------------------------------------------------- dispatching jobs
    def _any_idle(self):
        with self._lock:
            return any(ws.state == "ONLINE" and ws.conn is not None and not ws.jobs and not ws.stale_jobs
                       and self.scheduler.workers.get(w) == "IDLE" for w, ws in self.workers.items())

    def _dispatch_loop(self):
        while self.running:
            if not self._any_idle():
                time.sleep(0.2)
                continue
            job = self.store.next_queued(timeout=0.5)          # Person 1's blocking queue
            if job is None:
                continue
            with self._lock:
                wid = self.scheduler.get_available_worker()   # Person 2's round-robin choice
                ws = self.workers.get(wid) if wid else None
                if ws is None or ws.state != "ONLINE" or ws.conn is None or ws.jobs or ws.stale_jobs:
                    self.store.requeue(job.job_id)             # still QUEUED: just put it back in line
                    time.sleep(0.1)
                    continue
                self._assign(ws, job)

    def _assign(self, ws, job):
        attempt = job.attempts + 1
        payload = dict(job.payload)
        payload["job_id"] = job.job_id                         # same convention as Person 2's WorkerManager
        ok = ws.conn.send({"type": "job", "job_id": job.job_id, "attempt": attempt, "payload": payload})
        if not ok:
            self.store.requeue(job.job_id)
            self.log("could not send job %s to %s (connection broken) - put back in queue" % (job.job_id, ws.wid))
            return
        self.store.mark_running(job.job_id, ws.wid)
        ws.jobs[job.job_id] = attempt
        self._refresh_sched(ws)
        self.log("DISPATCH job %s (attempt %d) -> %s [%s]" % (job.job_id, attempt, ws.wid, ws.ip))

    # ---------------------------------------------------------------- failure detector + status file
    def _monitor_loop(self):
        last_status = 0.0
        while self.running:
            time.sleep(0.5)
            now = time.time()
            try:
                with self._lock:
                    for ws in list(self.workers.values()):
                        self._check_worker(ws, now)
                if now - last_status >= 1.0:
                    last_status = now
                    self._write_status()
            except Exception as e:                            # never let the detector die silently
                self.log("monitor loop error: %r" % (e,))

    def _check_worker(self, ws, now):
        if self.workers.get(ws.wid) is not ws:
            return
        if ws.state == "DISCONNECTED":
            if now >= ws.deadline:
                reach = self.sdn.host_reachable(ws.ip) if self.sdn else None
                kind = "NETWORK failure" if reach is False else "worker PROCESS failure"
                self._fail_worker(ws, "connection closed and worker did not return (%s)" % kind)
            return
        if ws.state not in ("ONLINE", "UNREACHABLE"):
            return
        age = now - ws.last_hb
        reach = self.sdn.host_reachable(ws.ip) if self.sdn else None
        recent_change = bool(self.sdn and self.sdn.recent_topology_change(self.topology_settle))

        if ws.state == "ONLINE":
            if reach is False and age > self.hb_interval * 1.5:
                self._mark_unreachable(ws, "controller reports NO PATH to %s" % ws.ip)
            elif age > self.hb_timeout:
                if reach is True and not recent_change:
                    self._fail_worker(ws, "no heartbeat for %.0fs while the controller says the path is UP -> worker PROCESS failure" % age)
                else:
                    self._mark_unreachable(ws, "no heartbeat for %.0fs and the network is suspect (%s)"
                                           % (age, "recent link change" if recent_change else "no controller data" if reach is None else "no path"))
        else:  # UNREACHABLE
            if now - ws.since > self.network_grace:
                self._fail_worker(ws, "network outage lasted more than %.0fs" % self.network_grace)
            elif age > self.hb_timeout + 4 and reach is True and not recent_change:
                self._fail_worker(ws, "path is UP again but worker stayed silent -> worker PROCESS failure")

    def _mark_unreachable(self, ws, why):
        ws.state, ws.since = "UNREACHABLE", time.time()
        self._set_sched(ws.wid, "UNREACHABLE")
        self.log("worker %s UNREACHABLE (%s). Holding its jobs %s for up to %.0fs; no new work is sent to it"
                 % (ws.wid, why, sorted(ws.jobs) or "(none)", self.network_grace))

    def _write_status(self):
        now = time.time()
        with self._lock:
            workers = {}
            for wid, ws in self.workers.items():
                workers[wid] = {"state": ws.state, "scheduler": self.scheduler.workers.get(wid), "ip": ws.ip,
                                "heartbeat_age_s": round(now - ws.last_hb, 1), "jobs": sorted(ws.jobs)}
            doc = {"timestamp": now, "stats": self.store.full_stats(), "workers": workers,
                   "events": list(self.events)[-15:]}
        tmp = self.status_file + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(doc, f)
            os.replace(tmp, self.status_file)
            os.chmod(self.status_file, 0o644)
        except OSError:
            pass
