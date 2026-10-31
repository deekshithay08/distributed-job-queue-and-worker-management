# Distributed Job Queue and Worker Management using SDN

## 1. Project Overview

This project implements a Distributed Job Queue and Worker Management System integrated with Software-Defined Networking (SDN).

The system allows clients to submit jobs to a central Job Manager, which maintains a job queue and distributes jobs among available workers for execution.

The project uses Mininet to create a virtual network topology and OS-KEN as the SDN controller. OpenFlow switches provide network connectivity between clients, the Job Manager, and remote workers.

The system is divided into three major components:

1. Client and Job Queue Management
2. Worker Management and Job Execution
3. SDN Network Management and Integration

## 2. Technologies Used

- **Programming Language:** Python 3
- **Networking Emulator:** Mininet
- **SDN Controller:** OS-KEN
- **Communication Protocol:** TCP
- **SDN Protocol:** OpenFlow
- **Operating System:** Ubuntu Linux
- **Network Topology:** Mininet with 4 OpenFlow switches and 5 hosts

## 3. Project Architecture

The system consists of the following components:

- **Client (c1, c2):** Submits jobs to the Job Manager.
- **Job Manager (jm):** Receives job requests, maintains the queue, assigns jobs to workers, and collects execution results.
- **Workers (w1, w2):** Register with the Job Manager, execute assigned jobs, and return results.
- **OS-KEN Controller:** Communicates with OpenFlow switches and manages network forwarding.
- **Mininet:** Creates the virtual network used to connect all components.

### Network Topology

The Mininet topology contains:

| Component | Host / Switch | IP Address |
|---|---|---|
| Job Manager | jm | 10.0.0.10 |
| Client 1 | c1 | 10.0.0.1 |
| Client 2 | c2 | 10.0.0.2 |
| Worker 1 | w1 | 10.0.0.21 |
| Worker 2 | w2 | 10.0.0.22 |
| Switches | s1, s2, s3, s4 | OpenFlow switches |

The controller listens on OpenFlow port **6653**.

### Job Execution Flow

1. A client submits a job to the Job Manager.
2. The Job Manager accepts the request and places the job in the queue.
3. Available workers register with the Job Manager.
4. The Job Manager dispatches queued jobs to workers.
5. Workers execute the assigned jobs.
6. Workers send execution results back to the Job Manager.
7. The client receives job completion information.

## 4. Work Distribution

### Person 1 – Client and Job Queue Management

Responsible for client communication and job queue functionality.

Main files:

| File | Description |
|---|---|
| `job_manager.py` | Handles client connections, job requests, queue management, job states, dispatching and results |
| `client.py` | Submits jobs and receives acknowledgements and results |
| `test_p1.py` | Tests for job queue and client functionality |

### Person 2 – Worker Management and Job Execution

Responsible for worker registration, job execution, worker availability and scheduling.

Main files:

| File | Description |
|---|---|
| `worker.py` | Contains job execution and processing logic |
| `worker_manager.py` | Manages local worker threads and their execution queues |
| `scheduler.py` | Handles worker registration, availability and worker selection |
| `test_worker.py` | Tests worker execution, job processing and scheduler functionality |

**Testing:**

Run the worker tests using:

```bash
python3 -m unittest -v test_worker
```

The test suite successfully passed 8 tests covering:
Test cases covered (8):

test_invalid – Checks handling of an invalid job operation.

test_invalid_sum_range – Checks handling of an invalid SUM range.

test_multiply – Tests the MULTIPLY operation.

test_job_failure – Tests failed job processing.

test_job_success – Tests successful job processing.

test_busy – Tests worker busy status.

test_round_robin – Tests round-robin worker selection.

test_sum – Tests the SUM operation.

Result: 8 test cases passed.

For the distributed Mininet implementation, remote worker execution is handled through `worker_node.py` and `remote_worker_manager.py`.

### Person 3 – SDN and Network Management

Responsible for creating the virtual network, SDN controller integration and network management.

Main files:

| File | Description |
|---|---|
| `controller.py` | OS-KEN SDN controller application |
| `run_controller.py` | Starts the OS-KEN controller |
| `topology.py` | Creates the Mininet network topology |
| `netconfig.py` | Stores network configuration and host information |
| `routing.py` | Contains network routing logic |
| `sdn_bridge.py` | Connects the SDN and job queue components |
| `remote_worker_manager.py` | Supports remote worker management |
| `monitor.py` | Network monitoring functionality |

## 5. Project Directory Structure

```text
sf_job_queue/
│
├── README.md
├── requirements.txt
│
├── job_manager.py
├── client.py
│
├── worker.py
├── worker_manager.py
├── scheduler.py
│
├── worker_node.py
├── remote_worker_manager.py
│
├── controller.py
├── run_controller.py
├── topology.py
├── netconfig.py
├── routing.py
├── sdn_bridge.py
├── monitor.py
│
├── test_p1.py
├── test_worker.py
├── tests/
│
├── cleanup.sh
└── logs/
```

## 6. Installation and Setup

### Requirements

- Ubuntu Linux
- Python 3
- Mininet
- OS-KEN
- VirtualBox (if running Ubuntu inside a virtual machine)

### Create and activate a virtual environment

```bash
python3 -m venv ~/cn_project/venv
source ~/cn_project/venv/bin/activate
```

### Install OS-KEN

```bash
pip install os-ken
```

If additional Python dependencies are required:

```bash
pip install -r requirements.txt
```

## 7. Running the Distributed System

All commands below assume the project is located at:

```bash
cd /media/sf_job_queue
```

### Step 1: Start the SDN Controller

Open Terminal 1:

```bash
cd /media/sf_job_queue
source ~/cn_project/venv/bin/activate
python run_controller.py
```

The controller listens on port 6653 for OpenFlow switches.

Keep this terminal running.

### Step 2: Start the Mininet Topology

Open Terminal 2:

```bash
cd /media/sf_job_queue
sudo mn -c
sudo python3 topology.py
```

Wait for the Mininet CLI:

```text
mininet-p3>
```

### Step 3: Verify Network Connectivity

Inside the Mininet CLI:

```text
pingall
```

Expected result:

```text
*** Results: 0% dropped
```

### Step 4: Start the Job Manager and Remote Workers

Inside the Mininet CLI:

```text
startapp
```

This starts:

- Job Manager on `jm`
- Worker 1 on `w1`
- Worker 2 on `w2`

Expected output:

```text
started run_job_manager.py on jm
started worker_node.py on w1
started worker_node.py on w2
```

### Step 5: Submit Jobs

Inside the Mininet CLI:

```text
submit c1 5 2.0
```

This submits five jobs from client `c1`, with an execution duration of approximately two seconds per job.

Expected output:

```text
ACK job=... state=QUEUED
DONE job=... state=COMPLETED
```

The client receives acknowledgements and completion messages for the submitted jobs.

## 8. Monitoring and Logs

The application generates log files inside the `logs/` directory.

### Job Manager logs

```bash
cat logs/jm.log
```

Displays worker registration, job queue activity, dispatching, execution results and statistics.

### Worker 1 logs

```bash
cat logs/worker1.log
```

Displays job execution activity on Worker 1.

### Worker 2 logs

```bash
cat logs/worker2.log
```

Displays job execution activity on Worker 2.

## 9. Demonstrated Results

The integrated D1 demonstration was successfully executed using Mininet.

### Network Connectivity

- Five hosts were connected through four OpenFlow switches.
- The switches connected to the OS-KEN controller.
- `pingall` completed with 0% packet loss.

### Worker Registration

Both remote workers successfully registered with the Job Manager:

```text
worker1 REGISTERED from 10.0.0.21
worker2 REGISTERED from 10.0.0.22
```

### Job Execution

Five jobs were submitted from client `c1`.

The jobs were distributed across the two workers:

| Worker | Jobs Completed |
|---|---:|
| Worker 1 | 3 |
| Worker 2 | 2 |
| **Total** | **5** |

### Final Job Manager Statistics

```text
queued: 0
running: 0
completed: 5
failed: 0
rejected: 0
```

All five submitted jobs were completed successfully.

## 10. Conclusion

The project demonstrates a distributed job queue and worker management system integrated with an SDN-based virtual network.

The Job Manager handles job submission and distribution, workers execute assigned tasks, and the OS-KEN controller manages communication with OpenFlow switches.

The successful Mininet demonstration verifies the integration of the client, Job Manager, remote workers and SDN network for the tested D1 workflow.
