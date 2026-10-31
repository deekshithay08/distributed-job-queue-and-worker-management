#!/usr/bin/env python3
"""
PERSON 3 - tiny launcher for the SDN controller.

Why this file exists: on Ubuntu 24.04 (Python 3.12) the classic "ryu-manager" command cannot be
installed from PyPI, and the maintained fork "os-ken" does not ship a "osken-manager" command.
This script does what ryu-manager does, in ~40 lines, and works with EITHER package
(os_ken is tried first, then ryu).

Usage:   python3 run_controller.py [path/to/app.py]      (default: controller.py next to this file)
"""
import logging
import os
import sys

try:
    import os_ken as _pkg
    PKG = "os_ken"
except ImportError:
    try:
        import ryu as _pkg
        PKG = "ryu"
    except ImportError:
        sys.exit("ERROR: neither 'os_ken' nor 'ryu' is installed.  See README step 'Install the controller'.")

import importlib

# Import order matters (the libraries have a circular import otherwise).
log = importlib.import_module(PKG + ".log")
log.early_init_log(logging.INFO)
importlib.import_module(PKG + ".flags")
app_manager = importlib.import_module(PKG + ".base.app_manager")
controller = importlib.import_module(PKG + ".controller.controller")   # registers --ofp-tcp-listen-port
hub = importlib.import_module(PKG + ".lib.hub")
cfg = importlib.import_module(PKG + ".cfg")

CONF = cfg.CONF


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    app_path = os.path.abspath(sys.argv[1]) if len(sys.argv) > 1 else os.path.join(here, "controller.py")
    if not os.path.isfile(app_path):
        sys.exit("ERROR: controller app not found: %s" % app_path)
    sys.path.insert(0, os.path.dirname(app_path))        # so the app can import netconfig.py
    CONF(args=[], project=PKG)                          # no command-line options; defaults (port 6653)
    log.init_log()
    hub.patch(thread=True)
    mgr = app_manager.AppManager.get_instance()
    mgr.load_apps([app_path])
    contexts = mgr.create_contexts()
    services = mgr.instantiate_apps(**contexts)
    print("[launcher] using package '%s'; OpenFlow listener on 0.0.0.0:6653; app=%s" % (PKG, app_path), flush=True)
    try:
        hub.joinall(services)
    except KeyboardInterrupt:
        print("\n[launcher] Ctrl+C - shutting down", flush=True)
    finally:
        mgr.close()


if __name__ == "__main__":
    main()
