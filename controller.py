"""
PERSON 3 - SDN controller (OpenFlow 1.3).   Run it with:   python3 run_controller.py

What it does (in plain words)
-----------------------------
1. FORWARDING   Knows the 4 switches, the links between them and where each host is plugged in
                (all from netconfig.py).  For every host it computes the cheapest path and installs
                one flow rule per switch:  "frames for MAC X  ->  send out port P".
2. ARP          Answers ARP requests itself ("ARP proxy") so no broadcast ever has to be flooded
                through the looped topology (no broadcast storms).
3. LINK EVENTS  Listens for OpenFlow PORT_STATUS messages.  When a port goes down/up it marks the
                link down/up, recomputes all paths and re-installs the rules.  This is the
                "re-routing".  Every 3 s it also asks the switches for their port list as a
                safety net in case an event was missed.
4. MONITORING   Every 2 s asks every switch for its port byte counters, converts them to bit/s,
                and writes everything to /tmp/sdn_status.json (read by monitor.py and by the job
                manager's sdn_bridge.py).  That file is how "the network tells the scheduler".
5. RECONNECT    If a switch (re)connects, its old rules are wiped and the correct ones re-installed.

Honest limitations: topology and host locations are static (from netconfig.py), they are not
discovered with LLDP.  Link failures are detected from port state, so a link that is "up" but
silently dropping packets is NOT detected by the controller (the job manager's heartbeats catch that).
"""
import json
import os
import time
from collections import deque

import netconfig as cfg
import routing

try:                                    # preferred on Ubuntu 24.04 / Python 3.12
    from os_ken.base import app_manager
    from os_ken.controller import ofp_event
    from os_ken.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
    from os_ken.ofproto import ofproto_v1_3
    from os_ken.lib import hub
    from os_ken.lib.packet import packet, ethernet, arp, ether_types
except ImportError:                     # classic Ryu (Ubuntu 22.04 and older with `pip install ryu`)
    from ryu.base import app_manager
    from ryu.controller import ofp_event
    from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
    from ryu.ofproto import ofproto_v1_3
    from ryu.lib import hub
    from ryu.lib.packet import packet, ethernet, arp, ether_types

BaseApp = getattr(app_manager, "OSKenApp", None) or app_manager.RyuApp

COOKIE = 0x50333            # tag on every rule we install ("P3")
PRIO_ROUTE = 10             # per-destination forwarding rules
PRIO_DROP = 5               # drop IPv6 noise so it never reaches the controller
PRIO_MISS = 0               # table-miss -> controller
REFRESH_EVERY = 30.0        # re-send all rules this often (self-healing, harmless)
PORTDESC_EVERY = 3.0        # reconcile link state this often


def now_str():
    return time.strftime("%H:%M:%S")


class P3Controller(BaseApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dps = {}                    # dpid -> datapath (switch currently connected)
        self.port_down = set()           # {(dpid, port_no)} ports that are administratively/physically down
        self.installed = {}              # dpid -> {host_name: out_port} what we believe is in the switch
        self.prev_stats = {}             # (dpid, port) -> (t, rx_bytes, tx_bytes)
        self.port_stats = {}             # (dpid, port) -> dict with counters + rates
        self.event_log = deque(maxlen=40)   # recent human-readable events (shown by monitor.py)
        self.started = time.time()
        self.last_topology_change = 0.0
        self.recompute_count = 0
        self.paths = {}                  # "jm->w1" -> ["s1","s2","s4"]
        self.reach = {}                  # host -> bool  (reachable from jm?)
        self.host_names = list(cfg.HOSTS.keys())
        self._known_links_up = {}        # link name -> bool  (to print only changes)
        hub.spawn(self._stats_loop)
        hub.spawn(self._portdesc_loop)
        hub.spawn(self._status_loop)
        hub.spawn(self._refresh_loop)
        self.log("controller started; waiting for switches on port %d" % cfg.OPENFLOW_PORT)

    # ------------------------------------------------------------------ helpers
    def log(self, msg):
        line = "[%s] [CTRL] %s" % (now_str(), msg)
        print(line, flush=True)
        self.event_log.append({"t": round(time.time(), 2), "msg": msg})

    @staticmethod
    def sname(dpid):
        return cfg.SWITCHES.get(dpid, "dp%s" % dpid)

    def link_is_up(self, name):
        for a, pa, b, pb, _c, n in cfg.LINKS:
            if n == name:
                return ((a, pa) not in self.port_down and (b, pb) not in self.port_down
                        and a in self.dps and b in self.dps)
        return False

    def host_port_up(self, host):
        h = cfg.HOSTS[host]
        return (h["dpid"], h["port"]) not in self.port_down

    # ------------------------------------------------------------------ switch connect / disconnect
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change(self, ev):
        dp = ev.datapath
        if dp is None:
            return
        if ev.state == MAIN_DISPATCHER:
            self.dps[dp.id] = dp
            self.installed[dp.id] = {}
            self.log("switch %s CONNECTED (dpid=%d)  -> wiping old rules, installing base rules" % (self.sname(dp.id), dp.id))
            self._clear_and_base_rules(dp)
            self._request_port_desc(dp)
            self.last_topology_change = time.time()
            self.recompute("switch %s connected" % self.sname(dp.id))
        elif ev.state == DEAD_DISPATCHER:
            if self.dps.get(dp.id) is dp:
                del self.dps[dp.id]
                self.installed.pop(dp.id, None)
                self.log("switch %s DISCONNECTED  -> its links are now considered DOWN" % self.sname(dp.id))
                self.last_topology_change = time.time()
                self.recompute("switch %s disconnected" % self.sname(dp.id))

    def _clear_and_base_rules(self, dp):
        ofp, p = dp.ofproto, dp.ofproto_parser
        dp.send_msg(p.OFPFlowMod(datapath=dp, command=ofp.OFPFC_DELETE, table_id=ofp.OFPTT_ALL,
                                 out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY, match=p.OFPMatch()))
        # table-miss: anything unknown goes to the controller (ARP requests, unknown unicast)
        self._add_flow(dp, PRIO_MISS, p.OFPMatch(),
                       [p.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)])
        # drop IPv6 (Mininet hosts chatter with it at start-up)
        self._add_flow(dp, PRIO_DROP, p.OFPMatch(eth_type=0x86DD), [])

    def _add_flow(self, dp, priority, match, actions):
        ofp, p = dp.ofproto, dp.ofproto_parser
        inst = [p.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)] if actions else []
        dp.send_msg(p.OFPFlowMod(datapath=dp, cookie=COOKIE, priority=priority, match=match, instructions=inst))

    def _del_route(self, dp, mac):
        ofp, p = dp.ofproto, dp.ofproto_parser
        dp.send_msg(p.OFPFlowMod(datapath=dp, command=ofp.OFPFC_DELETE_STRICT, priority=PRIO_ROUTE,
                                 out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
                                 match=p.OFPMatch(eth_dst=mac)))

    # ------------------------------------------------------------------ path computation + rule install
    def recompute(self, reason, force=False):
        """Recompute every route from the current link state and push only what changed."""
        self.recompute_count += 1
        routes, adj = routing.compute_routes(
            cfg.LINKS, cfg.HOSTS, self.link_is_up,
            switch_is_up=lambda d: d in self.dps,
            host_port_is_up=self.host_port_up)

        changed = 0
        for dpid, dp in list(self.dps.items()):
            have = self.installed.setdefault(dpid, {})
            for host in self.host_names:
                want = routes.get(host, {}).get(dpid)
                mac = cfg.HOSTS[host]["mac"]
                if want is None:
                    if host in have:
                        self._del_route(dp, mac)
                        del have[host]
                        changed += 1
                elif force or have.get(host) != want:
                    p = dp.ofproto_parser
                    self._add_flow(dp, PRIO_ROUTE, p.OFPMatch(eth_dst=mac), [p.OFPActionOutput(want)])
                    have[host] = want
                    changed += 1

        # ---- summaries for logging / status file
        jm = cfg.HOSTS["jm"]
        self.paths, self.reach = {}, {}
        for host in self.host_names:
            if host == "jm":
                continue
            h = cfg.HOSTS[host]
            path = None
            if self.host_port_up("jm") and self.host_port_up(host):
                path = routing.path_between(adj, jm["dpid"], h["dpid"]) if (jm["dpid"] in self.dps and h["dpid"] in self.dps) else None
            self.reach[host] = path is not None
            self.paths["jm->%s" % host] = [self.sname(d) for d in path] if path else None

        # print link up/down transitions once
        for a, pa, b, pb, _c, name in cfg.LINKS:
            up = self.link_is_up(name)
            if self._known_links_up.get(name, True) != up:
                self.log("LINK %s is now %s" % (name, "UP" if up else "DOWN"))
            self._known_links_up[name] = up

        if changed:
            wk = ", ".join("%s: %s" % (k, "-".join(v) if v else "NO PATH") for k, v in sorted(self.paths.items()) if k in ("jm->w1", "jm->w2", "jm->c1"))
            self.log("recompute (%s): %d rule(s) changed.  Paths now: %s" % (reason, changed, wk))
        self._write_status()

    # ------------------------------------------------------------------ port events
    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def _port_status(self, ev):
        msg, dp = ev.msg, ev.msg.datapath
        port = msg.desc
        if port.port_no > dp.ofproto.OFPP_MAX:
            return
        self._apply_port_state(dp, port, source="PORT_STATUS event")

    def _apply_port_state(self, dp, port, source):
        ofp = dp.ofproto
        down = bool(port.state & ofp.OFPPS_LINK_DOWN) or bool(port.config & ofp.OFPPC_PORT_DOWN)
        key = (dp.id, port.port_no)
        was_down = key in self.port_down
        if down == was_down:
            return
        if down:
            self.port_down.add(key)
        else:
            self.port_down.discard(key)
        self.last_topology_change = time.time()
        self.log("port %s:%d (%s) went %s   [%s]" % (self.sname(dp.id), port.port_no,
                                                       port.name.decode() if isinstance(port.name, bytes) else port.name,
                                                       "DOWN" if down else "UP", source))
        self.recompute("LINK/PORT change")

    def _request_port_desc(self, dp):
        dp.send_msg(dp.ofproto_parser.OFPPortDescStatsRequest(dp, 0))

    @set_ev_cls(ofp_event.EventOFPPortDescStatsReply, MAIN_DISPATCHER)
    def _port_desc_reply(self, ev):
        dp = ev.msg.datapath
        for port in ev.msg.body:
            if port.port_no <= dp.ofproto.OFPP_MAX:
                self._apply_port_state(dp, port, source="port-description poll")

    def _portdesc_loop(self):
        while True:
            hub.sleep(PORTDESC_EVERY)
            for dp in list(self.dps.values()):
                self._request_port_desc(dp)

    def _refresh_loop(self):
        while True:
            hub.sleep(REFRESH_EVERY)
            if self.dps:
                self.recompute("periodic refresh", force=True)

    # ------------------------------------------------------------------ packet-in : ARP proxy
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in(self, ev):
        msg = ev.msg
        dp = msg.datapath
        in_port = msg.match["in_port"]
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if eth is None:
            return
        a = pkt.get_protocol(arp.arp)
        if a is not None and a.opcode == arp.ARP_REQUEST:
            host = cfg.HOST_BY_IP.get(a.dst_ip)
            if host is None:
                return
            target_mac = cfg.HOSTS[host]["mac"]
            reply = packet.Packet()
            reply.add_protocol(ethernet.ethernet(ethertype=ether_types.ETH_TYPE_ARP, dst=eth.src, src=target_mac))
            reply.add_protocol(arp.arp(opcode=arp.ARP_REPLY, src_mac=target_mac, src_ip=a.dst_ip,
                                       dst_mac=a.src_mac, dst_ip=a.src_ip))
            reply.serialize()
            p, ofp = dp.ofproto_parser, dp.ofproto
            dp.send_msg(p.OFPPacketOut(datapath=dp, buffer_id=ofp.OFP_NO_BUFFER, in_port=ofp.OFPP_CONTROLLER,
                                       actions=[p.OFPActionOutput(in_port)], data=reply.data))
            return
        # anything else that reaches the controller has no route installed -> drop (rate-limited log)
        if eth.ethertype not in (ether_types.ETH_TYPE_LLDP, 0x86DD):
            key = ("unk", dp.id, eth.dst)
            t = time.time()
            if t - getattr(self, "_last_unk", {}).get(key, 0) > 10:
                self._last_unk = getattr(self, "_last_unk", {})
                self._last_unk[key] = t
                self.log("no route: dropped frame %s -> %s at %s port %d" % (eth.src, eth.dst, self.sname(dp.id), in_port))

    # ------------------------------------------------------------------ monitoring
    def _stats_loop(self):
        while True:
            hub.sleep(cfg.STATS_INTERVAL)
            for dp in list(self.dps.values()):
                p, ofp = dp.ofproto_parser, dp.ofproto
                dp.send_msg(p.OFPPortStatsRequest(dp, 0, ofp.OFPP_ANY))

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _stats_reply(self, ev):
        dp = ev.msg.datapath
        t = time.time()
        for st in ev.msg.body:
            if st.port_no > dp.ofproto.OFPP_MAX:
                continue
            key = (dp.id, st.port_no)
            rx_bps = tx_bps = 0.0
            if key in self.prev_stats:
                t0, rx0, tx0 = self.prev_stats[key]
                dt = max(t - t0, 0.001)
                rx_bps = max(st.rx_bytes - rx0, 0) * 8 / dt
                tx_bps = max(st.tx_bytes - tx0, 0) * 8 / dt
            self.prev_stats[key] = (t, st.rx_bytes, st.tx_bytes)
            self.port_stats[key] = {"rx_bytes": st.rx_bytes, "tx_bytes": st.tx_bytes,
                                    "rx_pkts": st.rx_packets, "tx_pkts": st.tx_packets,
                                    "rx_bps": round(rx_bps), "tx_bps": round(tx_bps)}

    # ------------------------------------------------------------------ status file (controller -> job manager / monitor.py)
    def _status_loop(self):
        n = 0
        while True:
            hub.sleep(1.0)
            self._write_status()
            n += 1
            if n % 10 == 0:
                busy = ["%s:%d tx=%.1f kb/s" % (self.sname(d), p, s["tx_bps"] / 1000.0)
                        for (d, p), s in sorted(self.port_stats.items()) if s["tx_bps"] > 2000]
                if busy:
                    self.log("traffic: " + "; ".join(busy[:8]))

    def _port_json(self, dpid, port):
        s = self.port_stats.get((dpid, port), {})
        return {"rx_bytes": s.get("rx_bytes", 0), "tx_bytes": s.get("tx_bytes", 0),
                "rx_bps": s.get("rx_bps", 0), "tx_bps": s.get("tx_bps", 0)}

    def _write_status(self):
        links = []
        for a, pa, b, pb, cost, name in cfg.LINKS:
            links.append({"name": name, "a": self.sname(a), "pa": pa, "b": self.sname(b), "pb": pb, "cost": cost,
                          "up": self.link_is_up(name),
                          "a_tx": self._port_json(a, pa), "b_tx": self._port_json(b, pb)})
        hosts = {}
        for name, h in cfg.HOSTS.items():
            hosts[name] = {"ip": h["ip"], "switch": self.sname(h["dpid"]), "port": h["port"],
                           "port_up": self.host_port_up(name), "reachable_from_jm": True if name == "jm" else bool(self.reach.get(name, False)),
                           "port_stats": self._port_json(h["dpid"], h["port"])}
        doc = {"timestamp": time.time(), "started": self.started, "last_topology_change": self.last_topology_change,
               "recompute_count": self.recompute_count,
               "switches_connected": sorted(self.sname(d) for d in self.dps),
               "links": links, "hosts": hosts, "paths": self.paths,
               "rules": {self.sname(d): dict(v) for d, v in self.installed.items()},
               "events": list(self.event_log)[-15:]}
        tmp = cfg.SDN_STATUS_FILE + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(doc, f)
            os.replace(tmp, cfg.SDN_STATUS_FILE)
            os.chmod(cfg.SDN_STATUS_FILE, 0o644)
        except OSError as e:
            print("[CTRL] cannot write status file:", e, flush=True)
