#!/usr/bin/env python3
"""
PERSON 3 - run_job_manager.py : start Person 1's job manager with REMOTE workers.

Person 1's job_manager.py has its own  `if __name__ == "__main__"`  that starts two *local threads*
as "workers".  Those would run on the job-manager host, not on w1/w2, so that entry point must not be
used in the Mininet demo.  This launcher imports Person 1's JobStore and client-facing server
UNCHANGED and swaps only the worker side:

        JobStore + serve()      (Person 1, port 5001, clients)           <- unchanged
        WorkerScheduler         (Person 2, round-robin IDLE/BUSY)         <- unchanged
        RemoteWorkerManager     (Person 3, port 5002, real TCP workers)   <- new

Run (inside the Mininet host 'jm'):   python3 run_job_manager.py
Ctrl+C saves metrics_job_manager.csv exactly like Person 1's version.
"""
import threading
import time

import netconfig as cfg
from job_manager import JobStore, serve, CLIENT_PORT          # Person 1 (unchanged)
from remote_worker_manager import RemoteWorkerManager
from sdn_bridge import SdnBridge


def main():
    store = JobStore()
    store.start_sampler()
    manager = RemoteWorkerManager(store, sdn=SdnBridge())
    manager.start()
    threading.Thread(target=serve, args=(store,), kwargs={"port": CLIENT_PORT}, daemon=True).start()
    try:
        while True:
            time.sleep(5)
            states = {w: s.state for w, s in manager.workers.items()}
            print("[%s] [JM] stats %s workers %s" % (time.strftime("%H:%M:%S"), store.full_stats(), states), flush=True)
    except KeyboardInterrupt:
        manager.stop()
        store.save_metrics("metrics_job_manager.csv")
        print("\n[JM] saved metrics_job_manager.csv")


if __name__ == "__main__":
    main()
