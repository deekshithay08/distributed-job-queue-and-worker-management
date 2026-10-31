"""
PERSON 1 - Client-facing job manager.

Owns:
  * TCP server that accepts multiple concurrent clients
  * job IDs, job queue, job states (QUEUED / RUNNING / COMPLETED / FAILED)
  * acknowledgment + result messages back to clients

Person 2 (workers/scheduler) plugs in through the JobStore methods:
    store.next_queued(timeout)        -> Job or None  (blocks until a job is waiting)
    store.mark_running(job_id, wid)
    store.mark_completed(job_id, result)
    store.mark_failed(job_id, reason)
    store.requeue(job_id)             -> used when a worker/path fails

Protocol (newline-delimited JSON over TCP, port 5001):
    client -> {"type":"submit_job","payload":{...}}
    server -> {"type":"ack","job_id":"ab12cd34","state":"QUEUED"}
    client -> {"type":"job_status","job_id":"ab12cd34"}
    server -> {"type":"status","job_id":"...","state":"RUNNING","worker_id":"w1"}
    server -> {"type":"result","job_id":"...","state":"COMPLETED","result":{...}}   (pushed)
    server -> {"type":"result","job_id":"...","state":"FAILED","error":"..."}       (pushed)
    server -> {"type":"error","message":"..."}                                       (bad request)

D2 additions:
    server -> {"type":"rejected","reason":"server busy","queue_len":N}   (queue full: backpressure)
    client -> {"type":"stats"}
    server -> {"type":"stats", queued, running, completed, failed, rejected, avg_latency_s, ...}
    Retry limit: a job that has been started MAX_ATTEMPTS times is FAILED instead of re-queued.
    Metrics: JobStore.start_sampler() records queue/running/... once per second;
             JobStore.save_metrics("file.csv") writes it out for the report graphs.
"""

import json
import queue
import socket
import threading
import time
import uuid

QUEUED, RUNNING, COMPLETED, FAILED = "QUEUED", "RUNNING", "COMPLETED", "FAILED"
CLIENT_PORT = 5001
DEFAULT_MAX_QUEUE = 100     # max jobs allowed to wait in QUEUED state
DEFAULT_MAX_ATTEMPTS = 3    # max times a job may be started before it is FAILED


class Job:
    def __init__(self, payload, client_addr):
        self.job_id = uuid.uuid4().hex[:8]
        self.payload = payload
        self.state = QUEUED
        self.client_addr = client_addr
        self.worker_id = None
        self.result = None
        self.error = None
        self.submitted_at = time.time()
        self.started_at = None
        self.finished_at = None
        self.attempts = 0

    def to_dict(self):
        return {"job_id": self.job_id, "state": self.state, "worker_id": self.worker_id,
                "attempts": self.attempts}


class JobStore:
    """Thread-safe job table + FIFO queue. Pushes results to clients on completion."""

    def __init__(self, max_queue=DEFAULT_MAX_QUEUE, max_attempts=DEFAULT_MAX_ATTEMPTS):
        self._lock = threading.Lock()
        self._jobs = {}          # job_id -> Job
        self._conns = {}         # job_id -> ClientConn that submitted it
        self._queue = queue.Queue()
        self.max_queue = max_queue
        self.max_attempts = max_attempts
        self.rejected = 0        # jobs turned away because the queue was full
        self.metrics = []        # rows sampled once per second (see start_sampler)
        self._t0 = time.time()

    # ---- used by the client server (Person 1) ----
    def queued_count(self):
        with self._lock:
            return sum(1 for j in self._jobs.values() if j.state == QUEUED)

    def submit(self, payload, conn):
        """Returns the new Job, or None if the queue is full (backpressure)."""
        job = Job(payload, conn.addr)
        with self._lock:
            waiting = sum(1 for j in self._jobs.values() if j.state == QUEUED)
            if waiting >= self.max_queue:
                self.rejected += 1
                return None
            self._jobs[job.job_id] = job
            self._conns[job.job_id] = conn
        self._queue.put(job.job_id)
        return job

    def get(self, job_id):
        with self._lock:
            return self._jobs.get(job_id)

    def stats(self):
        with self._lock:
            counts = {QUEUED: 0, RUNNING: 0, COMPLETED: 0, FAILED: 0}
            for j in self._jobs.values():
                counts[j.state] += 1
            return counts

    def full_stats(self):
        """Counts + rejected + average latency; also what a client gets for {"type":"stats"}."""
        with self._lock:
            counts = {QUEUED: 0, RUNNING: 0, COMPLETED: 0, FAILED: 0}
            lats = []
            for j in self._jobs.values():
                counts[j.state] += 1
                if j.state == COMPLETED:
                    lats.append(j.finished_at - j.submitted_at)
            return {"queued": counts[QUEUED], "running": counts[RUNNING],
                    "completed": counts[COMPLETED], "failed": counts[FAILED],
                    "rejected": self.rejected, "max_queue": self.max_queue,
                    "avg_latency_s": round(sum(lats) / len(lats), 3) if lats else None}

    # ---- metrics for the performance report ----
    def start_sampler(self, interval=1.0):
        def loop():
            while True:
                st = self.full_stats()
                self.metrics.append((round(time.time() - self._t0, 2), st["queued"], st["running"],
                                     st["completed"], st["failed"], st["rejected"]))
                time.sleep(interval)
        threading.Thread(target=loop, daemon=True).start()

    def save_metrics(self, filename):
        import csv
        with open(filename, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["t_s", "queued", "running", "completed", "failed", "rejected"])
            w.writerows(self.metrics)

    # ---- used by the scheduler / workers (Person 2) ----
    def running_jobs_of(self, worker_id):
        """IDs of all jobs currently RUNNING on `worker_id` (call this when that worker dies,
        then requeue() each one)."""
        with self._lock:
            return [j.job_id for j in self._jobs.values()
                    if j.state == RUNNING and j.worker_id == worker_id]

    def next_queued(self, timeout=None):
        """Block until a QUEUED job is available; returns Job or None on timeout."""
        while True:
            try:
                job_id = self._queue.get(timeout=timeout)
            except queue.Empty:
                return None
            with self._lock:
                job = self._jobs.get(job_id)
                if job and job.state == QUEUED:
                    return job
            # stale entry (already running/done) -> skip

    def mark_running(self, job_id, worker_id):
        with self._lock:
            job = self._jobs[job_id]
            job.state, job.worker_id = RUNNING, worker_id
            job.started_at = time.time()
            job.attempts += 1

    def mark_completed(self, job_id, result):
        with self._lock:
            job = self._jobs[job_id]
            job.state, job.result, job.finished_at = COMPLETED, result, time.time()
            conn = self._conns.get(job_id)
        if conn:
            conn.send({"type": "result", "job_id": job_id, "state": COMPLETED, "result": result,
                       "latency_s": round(job.finished_at - job.submitted_at, 3),
                       "attempts": job.attempts})

    def mark_failed(self, job_id, reason):
        with self._lock:
            job = self._jobs[job_id]
            job.state, job.error, job.finished_at = FAILED, reason, time.time()
            conn = self._conns.get(job_id)
        if conn:
            conn.send({"type": "result", "job_id": job_id, "state": FAILED, "error": reason})

    def requeue(self, job_id):
        """Put a RUNNING job back in the queue (worker or path failed).
        If it already used up max_attempts, mark it FAILED instead (retry limit).
        Returns True if re-queued, False if it was failed."""
        with self._lock:
            job = self._jobs[job_id]
            exhausted = job.attempts >= self.max_attempts
            if not exhausted:
                job.state, job.worker_id = QUEUED, None
        if exhausted:
            self.mark_failed(job_id, f"gave up after {job.attempts} attempts")
            return False
        self._queue.put(job_id)
        return True


class ClientConn:
    """One connected client; serialises sends from multiple threads."""

    def __init__(self, sock, addr):
        self.sock, self.addr = sock, addr
        self._send_lock = threading.Lock()

    def send(self, obj):
        try:
            with self._send_lock:
                self.sock.sendall((json.dumps(obj) + "\n").encode())
        except OSError:
            pass   # client went away; job still tracked in the store


def handle_client(sock, addr, store):
    conn = ClientConn(sock, addr)
    print(f"[JM] client connected {addr}")
    buf = b""
    try:
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                try:
                    msg = json.loads(line.decode())
                except json.JSONDecodeError:
                    conn.send({"type": "error", "message": "invalid JSON"})
                    continue
                process_message(msg, conn, store)
    except (ConnectionError, OSError):
        pass
    finally:
        sock.close()
        print(f"[JM] client disconnected {addr}")


def process_message(msg, conn, store):
    mtype = msg.get("type")
    if mtype == "submit_job":
        if not isinstance(msg.get("payload"), dict):
            conn.send({"type": "error", "message": "submit_job needs a 'payload' object"})
            return
        job = store.submit(msg["payload"], conn)
        if job is None:
            conn.send({"type": "rejected", "reason": "server busy", "queue_len": store.max_queue})
            print(f"[JM] REJECTED job from {conn.addr}: queue full ({store.max_queue})")
            return
        conn.send({"type": "ack", "job_id": job.job_id, "state": job.state})
        print(f"[JM] {job.job_id} QUEUED from {conn.addr}  queue={store.stats()}")
    elif mtype == "stats":
        conn.send({"type": "stats", **store.full_stats()})
    elif mtype == "job_status":
        job = store.get(msg.get("job_id"))
        if job is None:
            conn.send({"type": "error", "message": "unknown job_id"})
        else:
            conn.send({"type": "status", **job.to_dict()})
    else:
        conn.send({"type": "error", "message": f"unknown message type {mtype!r}"})


def serve(store, host="0.0.0.0", port=CLIENT_PORT):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(100)
    print(f"[JM] accepting clients on {host}:{port}")
    while True:
        sock, addr = srv.accept()
        threading.Thread(target=handle_client, args=(sock, addr, store), daemon=True).start()



if __name__ == "__main__":
    from worker_manager import WorkerManager

    store = JobStore()
    store.start_sampler()

    worker_manager = WorkerManager(store, number_of_workers=2)
    worker_manager.start()

    threading.Thread(target=serve, args=(store,), daemon=True).start()

    try:
        while True:
            time.sleep(5)
            print(f"[JM] stats {store.full_stats()}")

    except KeyboardInterrupt:
        worker_manager.stop()
        store.save_metrics("metrics_job_manager.csv")
        print("\n[JM] saved metrics_job_manager.csv")

