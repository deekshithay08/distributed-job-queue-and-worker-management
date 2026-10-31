#!/usr/bin/env python3
"""
PERSON 3 - Mininet topology.   Run with:   sudo python3 topology.py            (kernel OVS, normal VM)
                                           sudo python3 topology.py --userspace  (only if the kernel
                                                                   Open vSwitch module is missing)

Creates 4 OpenFlow switches, 5 hosts and the redundant diamond described in netconfig.py, connects
the switches to the remote controller (started separately with run_controller.py), then opens a
Mininet CLI with a few extra commands:

    startapp                 start the job manager on 'jm' and the worker programs on 'w1','w2'
    submit c1 5 2.0          run Person 1's client.py on host c1: 5 jobs of 2.0 s (waits until done)
    submitbg c1 5 2.0        same, but in the background (log: logs/client_c1.log)
    killworker w1            kill -9 the worker program on w1  (simulated worker crash)
    startworker w1           start the worker program on w1 again
    status                   print the monitoring dashboard (same as monitor.py)
    stopapp                  stop jm + workers
and all normal Mininet commands:   pingall | link s1 s2 down | link s1 s2 up | w1 ping jm | exit
"""
import argparse
import os
import signal
import subprocess
import sys
import time

import netconfig as cfg

from mininet.cli import CLI
from mininet.log import info, setLogLevel
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController

PY = sys.executable or "python3"
APP_PROCS = {}            # name -> subprocess.Popen   (processes running inside Mininet hosts)


# ------------------------------------------------------------------------------------------ network
def build_network(userspace=False, controller_ip="127.0.0.1", controller_port=cfg.OPENFLOW_PORT):
    net = Mininet(controller=None, switch=OVSSwitch, autoSetMacs=False, autoStaticArp=False, build=False)
    net.addController("c0", controller=RemoteController, ip=controller_ip, port=controller_port)

    switches = {}
    for dpid, name in cfg.SWITCHES.items():
        kw = dict(cls=OVSSwitch, protocols="OpenFlow13", dpid="%016x" % dpid, failMode="secure")
        if userspace:
            kw["datapath"] = "user"
        switches[dpid] = net.addSwitch(name, **kw)

    for name, h in cfg.HOSTS.items():
        host = net.addHost(name, ip="%s/%d" % (h["ip"], cfg.SUBNET_PREFIX), mac=h["mac"])
        net.addLink(host, switches[h["dpid"]], port1=0, port2=h["port"])

    for a, pa, b, pb, _cost, _name in cfg.LINKS:
        net.addLink(switches[a], switches[b], port1=pa, port2=pb)

    net.build()
    # Turn off NIC checksum/segmentation offload on the virtual host interfaces.  With Open vSwitch's
    # userspace datapath (and on some VirtualBox setups) frames with "to be computed later" checksums are
    # dropped, which makes ping work but TCP time out.  Harmless otherwise.  Needs the 'ethtool' package.
    for h in net.hosts:
        h.cmd("ethtool -K %s-eth0 tx off rx off tso off gso off gro off 2>/dev/null" % h.name)
    return net


def wait_until_ready(net, timeout=25):
    """Wait for all switches to connect, then until the controller has installed routes."""
    info("*** Waiting for the 4 switches to connect to the controller...\n")
    net.waitConnected(timeout=timeout)
    jm = net.get("jm")
    t0 = time.time()
    while time.time() - t0 < timeout:
        ok = all("1 received" in jm.cmd("ping -c1 -W1 %s" % cfg.HOSTS[h]["ip"]) for h in ("w1", "w2", "c1"))
        if ok:
            info("*** Network is forwarding (jm can reach c1, w1, w2)\n")
            return True
        time.sleep(1)
    info("*** WARNING: jm cannot reach all hosts yet. Is run_controller.py running? See README troubleshooting.\n")
    return False


# ------------------------------------------------------------------------------------------ apps inside hosts
def _logfile(name):
    os.makedirs(cfg.LOG_DIR, exist_ok=True)
    path = os.path.join(cfg.LOG_DIR, name + ".log")
    f = open(path, "a", buffering=1)
    f.write("\n===== %s started %s =====\n" % (name, time.strftime("%Y-%m-%d %H:%M:%S")))
    return f


def _start(net, host, name, script, args=()):
    if name in APP_PROCS and APP_PROCS[name].poll() is None:
        info("%s is already running (pid %d)\n" % (name, APP_PROCS[name].pid))
        return APP_PROCS[name]
    lf = _logfile(name)
    proc = net.get(host).popen([PY, "-u", os.path.join(cfg.PERSON3_DIR, script)] + list(args),
                               stdout=lf, stderr=subprocess.STDOUT, cwd=cfg.PERSON3_DIR)
    APP_PROCS[name] = proc
    info("started %s on %s (pid %d), log: logs/%s.log\n" % (script, host, proc.pid, name))
    return proc


def start_jm(net):
    return _start(net, "jm", "jm", "run_job_manager.py")


def start_worker(net, host):
    wid = cfg.WORKER_IDS[host]
    return _start(net, host, wid, "worker_node.py", [wid, cfg.JM_IP])


def kill_worker(host, sig=signal.SIGKILL):
    wid = cfg.WORKER_IDS[host]
    p = APP_PROCS.get(wid)
    if p and p.poll() is None:
        p.send_signal(sig)
        p.wait(timeout=5)
        info("killed %s on %s (signal %s)\n" % (wid, host, int(sig)))
        return True
    info("%s is not running\n" % wid)
    return False


def stop_apps():
    for name, p in list(APP_PROCS.items()):
        if p.poll() is None:
            p.send_signal(signal.SIGINT if name == "jm" else signal.SIGTERM)
    time.sleep(0.5)
    for name, p in list(APP_PROCS.items()):
        if p.poll() is None:
            p.kill()
    APP_PROCS.clear()


def start_app(net):
    start_jm(net)
    time.sleep(1.0)
    for h in ("w1", "w2"):
        start_worker(net, h)


# ------------------------------------------------------------------------------------------ CLI
class P3CLI(CLI):
    prompt = "mininet-p3> "

    def do_startapp(self, _line):
        """startapp : start job manager (jm) and workers (w1, w2)"""
        start_app(self.mn)

    def do_stopapp(self, _line):
        """stopapp : stop job manager and workers"""
        stop_apps()

    def do_killworker(self, line):
        """killworker w1 : kill -9 the worker program on that host"""
        h = line.strip() or "w1"
        if h not in cfg.WORKER_IDS:
            print("usage: killworker w1|w2")
            return
        kill_worker(h)

    def do_startworker(self, line):
        """startworker w1 : (re)start the worker program on that host"""
        h = line.strip() or "w1"
        if h not in cfg.WORKER_IDS:
            print("usage: startworker w1|w2")
            return
        start_worker(self.mn, h)

    def do_submit(self, line):
        """submit c1 5 2.0 : run Person 1's client.py on host c1 (5 jobs, 2.0 s each). Blocks until done."""
        parts = line.split()
        host = parts[0] if parts else "c1"
        jobs = parts[1] if len(parts) > 1 else "3"
        dur = parts[2] if len(parts) > 2 else "1.0"
        script = os.path.join(cfg.JOB_QUEUE_DIR, "client.py")
        print(self.mn.get(host).cmd("%s -u %s %s %s %s %s" % (PY, script, cfg.JM_IP, jobs, dur, host)))

    def do_submitbg(self, line):
        """submitbg c1 5 2.0 : like 'submit' but runs in the background (output -> logs/client_c1.log), so you can break a link meanwhile"""
        parts = line.split()
        host = parts[0] if parts else "c1"
        jobs = parts[1] if len(parts) > 1 else "3"
        dur = parts[2] if len(parts) > 2 else "1.0"
        lf = _logfile("client_" + host)
        self.mn.get(host).popen([PY, "-u", os.path.join(cfg.JOB_QUEUE_DIR, "client.py"), cfg.JM_IP, jobs, dur, host],
                                stdout=lf, stderr=subprocess.STDOUT)
        print("running in background; watch it with:  tail -f logs/client_%s.log" % host)

    def do_status(self, _line):
        """status : print the monitoring dashboard once"""
        import monitor
        print(monitor.render())


# ------------------------------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--userspace", action="store_true", help="use the userspace OVS datapath (no kernel module)")
    ap.add_argument("--controller-ip", default="127.0.0.1")
    ap.add_argument("--autostart", action="store_true", help="start jm + workers automatically")
    args = ap.parse_args()

    if os.geteuid() != 0:
        sys.exit("Mininet needs root:  sudo python3 topology.py")
    for pat in ("run_job_manager[.]py", "worker_node[.]py"):      # leftovers from a crashed earlier run
        subprocess.call(["pkill", "-f", pat])
    setLogLevel("info")
    net = build_network(args.userspace, args.controller_ip)
    net.start()
    try:
        wait_until_ready(net)
        if args.autostart:
            start_app(net)
        info("\n*** Type 'startapp' to start the job manager and workers, 'help' for commands, 'exit' to quit.\n")
        P3CLI(net)
    finally:
        stop_apps()
        net.stop()


if __name__ == "__main__":
    main()
