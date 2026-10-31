#!/usr/bin/env python3
"""
PERSON 3 - monitor.py : one-screen dashboard of NETWORK + APPLICATION state.

    python3 monitor.py            print once
    python3 monitor.py --watch    refresh every 2 s (Ctrl+C to stop)

It does not touch the network.  It only reads the two small JSON files that the controller
(/tmp/sdn_status.json) and the job manager (/tmp/jm_status.json) write once per second.
"""
import json
import sys
import time

import netconfig as cfg


def _load(path):
    try:
        with open(path) as f:
            d = json.load(f)
        d["_age"] = time.time() - d.get("timestamp", 0)
        return d
    except (OSError, ValueError):
        return None


def _bps(v):
    return "%.1f kb/s" % (v / 1000.0) if v < 1e6 else "%.2f Mb/s" % (v / 1e6)


def render():
    sdn, jm = _load(cfg.SDN_STATUS_FILE), _load(cfg.JM_STATUS_FILE)
    out = ["=" * 78, "  MONITOR   %s" % time.strftime("%Y-%m-%d %H:%M:%S"), "=" * 78]

    out.append("\n-- NETWORK (from SDN controller) " + "-" * 44)
    if sdn is None:
        out.append("  no controller status file: is run_controller.py running?")
    else:
        if sdn["_age"] > cfg.SDN_STALE_AFTER:
            out.append("  WARNING: controller status is %.0f s old (controller stopped?)" % sdn["_age"])
        out.append("  switches connected: %s   (route recomputations: %d)" % (", ".join(sdn["switches_connected"]) or "none", sdn["recompute_count"]))
        out.append("  %-8s %-6s %-14s %-14s %s" % ("LINK", "STATE", "tx a->b", "tx b->a", "cost"))
        for l in sdn["links"]:
            out.append("  %-8s %-6s %-14s %-14s %d" % (l["name"], "UP" if l["up"] else "DOWN",
                                                     _bps(l["a_tx"]["tx_bps"]), _bps(l["b_tx"]["tx_bps"]), l["cost"]))
        out.append("  current forwarding paths from jm:")
        for k, v in sorted(sdn["paths"].items()):
            out.append("    %-7s %s" % (k, " -> ".join(v) if v else "NO PATH"))
        out.append("  hosts: " + "   ".join("%s[%s]=%s" % (n, h["switch"], "ok" if (h["port_up"] and h["reachable_from_jm"]) else "UNREACHABLE")
                                            for n, h in sdn["hosts"].items()))
        total = sum(l["a_tx"]["tx_bytes"] + l["b_tx"]["tx_bytes"] for l in sdn["links"])
        out.append("  total bytes carried on inter-switch links: %d" % total)

    out.append("\n-- APPLICATION (from job manager) " + "-" * 43)
    if jm is None:
        out.append("  no job-manager status file: is run_job_manager.py running?")
    else:
        if jm["_age"] > 5:
            out.append("  WARNING: job-manager status is %.0f s old" % jm["_age"])
        s = jm["stats"]
        out.append("  jobs: queued=%d running=%d completed=%d failed=%d rejected=%d  avg_latency=%s s"
                   % (s["queued"], s["running"], s["completed"], s["failed"], s["rejected"], s["avg_latency_s"]))
        out.append("  %-9s %-12s %-10s %-10s %s" % ("WORKER", "STATE", "SCHEDULER", "HB AGE", "JOBS"))
        for wid, w in sorted(jm["workers"].items()):
            out.append("  %-9s %-12s %-10s %-10s %s" % (wid, w["state"], w["scheduler"], "%.1fs" % w["heartbeat_age_s"], ",".join(w["jobs"]) or "-"))
        if not jm["workers"]:
            out.append("  (no workers registered yet)")

    ev = []
    if sdn:
        ev += [(e["t"], "CTRL", e["msg"]) for e in sdn.get("events", [])]
    if jm:
        ev += [(e["t"], "JM  ", e["msg"]) for e in jm.get("events", [])]
    out.append("\n-- RECENT EVENTS (both components, newest last) " + "-" * 29)
    for t, who, msg in sorted(ev)[-12:]:
        out.append("  %s [%s] %s" % (time.strftime("%H:%M:%S", time.localtime(t)), who, msg[:110]))
    return "\n".join(out)


if __name__ == "__main__":
    if "--watch" in sys.argv:
        try:
            while True:
                print("\033[2J\033[H" + render(), flush=True)
                time.sleep(2)
        except KeyboardInterrupt:
            pass
    else:
        print(render())
