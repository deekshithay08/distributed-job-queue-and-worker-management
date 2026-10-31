"""
PERSON 3 - sdn_bridge.py : how network information reaches the job manager's scheduling logic.

The controller writes /tmp/sdn_status.json once a second (see controller.py).  This class reads it.
All answers are three-valued so the caller never mistakes "I don't know" for "fine":

    host_reachable(ip)  ->  True   controller says a path from jm to that host exists and its port is up
                            False  controller says the host is cut off (no path / port down)
                            None   no fresh controller data (controller not running or file too old)
"""
import json
import time

import netconfig as cfg


class SdnBridge:
    def __init__(self, path=cfg.SDN_STATUS_FILE, stale_after=cfg.SDN_STALE_AFTER):
        self.path = path
        self.stale_after = stale_after
        self._cache, self._cache_t = None, 0.0

    def _load(self):
        now = time.time()
        if now - self._cache_t < 0.3:
            return self._cache
        self._cache_t = now
        try:
            with open(self.path) as f:
                self._cache = json.load(f)
        except (OSError, ValueError):
            self._cache = None
        return self._cache

    def doc(self):
        """Latest status dict, or None if the controller is not publishing fresh data."""
        d = self._load()
        if d is None or time.time() - d.get("timestamp", 0) > self.stale_after:
            return None
        return d

    def host_reachable(self, ip):
        d = self.doc()
        if d is None:
            return None
        for h in d.get("hosts", {}).values():
            if h.get("ip") == ip:
                return bool(h.get("port_up", True)) and bool(h.get("reachable_from_jm", False))
        return None

    def recent_topology_change(self, within):
        d = self.doc()
        if d is None:
            return False
        return time.time() - d.get("last_topology_change", 0) < within
