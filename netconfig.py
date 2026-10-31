"""
PERSON 3 - ONE place that defines every IP address, port number, switch port and link.

topology.py, controller.py, worker_node.py, run_job_manager.py, monitor.py and the tests ALL import
this file, so the whole project always uses the same numbers.

Network picture (all hosts are in one subnet 10.0.0.0/24):

                    +--------- s2 ---------+        PRIMARY path   : s1 - s2 - s4  (cost 1 + 1)
                    |  (link A)   (link B) |
      jm ---- s1 ---+                      +--- s4 ---- c1, c2, w1, w2
                    |  (link C)   (link D) |
                    +--------- s3 ---------+        ALTERNATE path: s1 - s3 - s4  (cost 10 + 10)

Only the controller's *routing decision* (cheapest path) makes s2 the primary; both paths are real
links, so when the primary breaks, traffic really moves to s3.
"""
import os
import sys

# --------------------------------------------------------------------------------------------
# Where the teammates' code lives.  Default: a folder called "job_queue" next to "person3".
# Importing this file adds that folder to sys.path so   `import job_manager`, `import worker`,
# `import scheduler`, `import client`   work unchanged.
# --------------------------------------------------------------------------------------------
PERSON3_DIR = os.path.dirname(os.path.abspath(__file__))
JOB_QUEUE_DIR = "/media/sf_job_queue"
for _p in (PERSON3_DIR, JOB_QUEUE_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

LOG_DIR = os.path.join(PERSON3_DIR, "logs")

# --------------------------------------------------------------------------------------------
# TCP ports  (who LISTENS on each)
# --------------------------------------------------------------------------------------------
CLIENT_PORT = 5001        # job manager listens; clients connect.  (Person 1's existing port.)
WORKER_PORT = 5002        # job manager listens; workers connect.  (NEW, Person 3's worker protocol.)
OPENFLOW_PORT = 6653      # SDN controller listens; the 4 switches connect.  (control plane only)

# --------------------------------------------------------------------------------------------
# Out-of-band "management" files (controller -> job manager -> monitor.py).
# Mininet hosts share the VM's file system, so a file is a simple, honest stand-in for a
# separate management network.  They are NOT part of the data plane.
# --------------------------------------------------------------------------------------------
SDN_STATUS_FILE = "/tmp/sdn_status.json"   # written by controller.py once per second
JM_STATUS_FILE = "/tmp/jm_status.json"     # written by the job manager once per second

# --------------------------------------------------------------------------------------------
# Hosts.  Fixed MAC = 00:00:00:00:00:<last two digits of ip, in hex>  (set by topology.py).
#   dpid / port = which switch and which switch-port the host is plugged into.
# --------------------------------------------------------------------------------------------
HOSTS = {
    "jm": {"ip": "10.0.0.10", "mac": "00:00:00:00:00:0a", "dpid": 1, "port": 1},
    "c1": {"ip": "10.0.0.1",  "mac": "00:00:00:00:00:01", "dpid": 4, "port": 1},
    "c2": {"ip": "10.0.0.2",  "mac": "00:00:00:00:00:02", "dpid": 4, "port": 2},
    "w1": {"ip": "10.0.0.21", "mac": "00:00:00:00:00:15", "dpid": 4, "port": 3},
    "w2": {"ip": "10.0.0.22", "mac": "00:00:00:00:00:16", "dpid": 4, "port": 4},
}
JM_IP = HOSTS["jm"]["ip"]
SUBNET_PREFIX = 24

# Worker id used by Person 2's code ("worker1", "worker2") <-> Mininet host name
WORKER_IDS = {"w1": "worker1", "w2": "worker2"}
WORKER_HOST_OF = {v: k for k, v in WORKER_IDS.items()}
WORKER_IP_OF = {WORKER_IDS[h]: HOSTS[h]["ip"] for h in WORKER_IDS}

# --------------------------------------------------------------------------------------------
# Switches and inter-switch links.
#   (dpid_a, port_on_a, dpid_b, port_on_b, cost, name)
# Port numbers are forced in topology.py (addLink(port1=..., port2=...)), so the controller can
# know them in advance.  Lowest total cost wins => primary path is s1-s2-s4.
# --------------------------------------------------------------------------------------------
SWITCHES = {1: "s1", 2: "s2", 3: "s3", 4: "s4"}
LINKS = [
    (1, 2, 2, 1, 1,  "s1-s2"),    # link A  (primary)
    (2, 2, 4, 5, 1,  "s2-s4"),    # link B  (primary)
    (1, 3, 3, 1, 10, "s1-s3"),    # link C  (alternate)
    (3, 2, 4, 6, 10, "s3-s4"),    # link D  (alternate)
]

# --------------------------------------------------------------------------------------------
# Timers (seconds).  Kept here so the job manager, worker and tests agree.
# --------------------------------------------------------------------------------------------
HEARTBEAT_INTERVAL = 2.0     # worker -> job manager
HEARTBEAT_TIMEOUT = 6.0      # job manager: no heartbeat for this long => worker is silent
DISCONNECT_GRACE = 2.0       # after a TCP close, wait this long for the worker to come back
NETWORK_GRACE = float(os.environ.get("P3_NETWORK_GRACE", 20))   # if the NETWORK is to blame, keep a silent worker's jobs this long
SDN_STALE_AFTER = 5.0        # controller status older than this is ignored
TOPOLOGY_SETTLE = 12.0       # a link event younger than this makes a silent worker "suspect network"
STATS_INTERVAL = 2.0         # controller polls port counters this often

HOST_BY_IP = {h["ip"]: n for n, h in HOSTS.items()}
HOST_BY_MAC = {h["mac"]: n for n, h in HOSTS.items()}
