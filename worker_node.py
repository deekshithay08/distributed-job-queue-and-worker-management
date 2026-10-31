#!/usr/bin/env python3
"""
PERSON 3 - worker_node.py : the program that runs ON a worker host (w1 / w2).

    python3 worker_node.py worker1 10.0.0.10          # worker id, job-manager IP  [, port [, heartbeat_seconds]]

It connects OVER THE NETWORK to the job manager (TCP 5002), registers, sends a heartbeat every 2 s,
receives jobs and runs them with Person 2's process_job() (worker.py, unchanged), and sends results back.

Survives network trouble:
  * no heartbeat_ack for ACK_TIMEOUT seconds  -> close the socket and reconnect
  * the job keeps running while disconnected; its result is kept in `unsent` and re-sent after
    reconnecting (the job manager decides whether it is still wanted)
"""
import json
import queue
import socket
import sys
import threading
import time

import netconfig as cfg
from worker import process_job                 # Person 2 (unchanged): the actual job execution

ACK_TIMEOUT = 6.0


def now_str():
    return time.strftime("%H:%M:%S")


class WorkerNode:
    def __init__(self, worker_id, jm_ip, port=cfg.WORKER_PORT, hb_interval=cfg.HEARTBEAT_INTERVAL):
        self.wid, self.jm_ip, self.port, self.hb_interval = worker_id, jm_ip, port, hb_interval
        self.job_q = queue.Queue()
        self.current = None            # job message being executed
        self.unsent = []               # finished results not yet acknowledged by the job manager
        self.lock = threading.Lock()
        self.send_lock = threading.Lock()
        self.sock = None
        threading.Thread(target=self._executor, daemon=True).start()

    def log(self, msg):
        print("[%s] [%s] %s" % (now_str(), self.wid, msg), flush=True)

    # ------------------------------------------------------------ sending
    def _send(self, obj):
        with self.send_lock:
            if self.sock is None:
                return False
            try:
                self.sock.sendall((json.dumps(obj) + "\n").encode())
                return True
            except OSError:
                return False

    # ------------------------------------------------------------ job execution (separate thread so heartbeats continue)
    def _executor(self):
        while True:
            job = self.job_q.get()
            with self.lock:
                self.current = job
            self.log("START job %s (attempt %s) payload=%s" % (job["job_id"], job["attempt"], job["payload"]))
            try:
                res = process_job(dict(job["payload"]))
            except Exception as e:                    # anything unexpected -> report FAILED instead of dying
                res = {"status": "FAILED", "error": repr(e)}
            msg = {"type": "result", "worker_id": self.wid, "job_id": job["job_id"], "attempt": job["attempt"],
                   "status": res["status"], "result": res.get("result"), "error": res.get("error")}
            with self.lock:
                self.unsent.append(msg)
                self.current = None
            sent = self._send(msg)
            self.log("FINISHED job %s -> %s (%s)" % (job["job_id"], res["status"], "sent" if sent else "NOT sent, will retry after reconnect"))

    def _running_list(self):
        with self.lock:
            jobs = ([self.current] if self.current else []) + list(self.job_q.queue)
        return [{"job_id": j["job_id"], "attempt": j["attempt"]} for j in jobs]

    # ------------------------------------------------------------ connection / session
    def run_forever(self):
        while True:
            try:
                sock = socket.create_connection((self.jm_ip, self.port), timeout=3)
            except OSError as e:
                self.log("cannot reach job manager %s:%d (%s) - retrying in 2 s" % (self.jm_ip, self.port, e))
                time.sleep(2)
                continue
            try:
                self._session(sock)
            except Exception as e:
                self.log("session error: %r" % (e,))
            finally:
                with self.send_lock:
                    self.sock = None
                try:
                    sock.close()
                except OSError:
                    pass
            time.sleep(1)

    def _session(self, sock):
        sock.settimeout(0.5)
        with self.send_lock:
            self.sock = sock
        with self.lock:
            unsent = list(self.unsent)
        self._send({"type": "register", "worker_id": self.wid, "running": self._running_list(), "unsent_results": unsent})
        self.log("connected to job manager %s:%d, registering (%d unsent result(s))" % (self.jm_ip, self.port, len(unsent)))
        last_ack, last_hb, buf = time.time(), 0.0, b""
        while True:
            now = time.time()
            if now - last_hb >= self.hb_interval:
                with self.lock:
                    cur = self.current["job_id"] if self.current else None
                if not self._send({"type": "heartbeat", "worker_id": self.wid, "busy": cur is not None, "job_id": cur}):
                    self.log("send failed -> reconnecting")
                    return
                last_hb = now
            if now - last_ack > ACK_TIMEOUT:
                self.log("no reply from job manager for %.0fs -> closing and reconnecting" % (now - last_ack))
                return
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                continue
            except OSError:
                self.log("connection error -> reconnecting")
                return
            if not chunk:
                self.log("job manager closed the connection -> reconnecting")
                return
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                try:
                    msg = json.loads(line.decode())
                except ValueError:
                    continue
                t = msg.get("type")
                if t in ("registered", "heartbeat_ack"):
                    last_ack = time.time()
                elif t == "job":
                    last_ack = time.time()
                    self.job_q.put(msg)
                elif t == "result_ack":
                    last_ack = time.time()
                    with self.lock:
                        self.unsent = [m for m in self.unsent if m["job_id"] != msg.get("job_id")]


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    WorkerNode(sys.argv[1], sys.argv[2],
               int(sys.argv[3]) if len(sys.argv) > 3 else cfg.WORKER_PORT,
               float(sys.argv[4]) if len(sys.argv) > 4 else cfg.HEARTBEAT_INTERVAL).run_forever()
