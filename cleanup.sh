#!/bin/bash
# PERSON 3 - clean up everything this project may have left running.  Safe to run any time.
sudo mn -c                                   # remove Mininet hosts, links, switches
pkill -f "run_controller[.]py" 2>/dev/null   # stop the SDN controller
pkill -f "run_job_manager[.]py" 2>/dev/null
pkill -f "worker_node[.]py" 2>/dev/null
rm -f /tmp/sdn_status.json /tmp/jm_status.json
echo "cleanup done"
