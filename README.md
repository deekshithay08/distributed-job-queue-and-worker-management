# Person 1 - Client & Job Queue (D1 + D2)

## Files
| File | What it is |
|---|---|
| `job_manager.py` | TCP server (many clients), job IDs, queue, 4 job states, ACK/result messages, retry limit, queue cap ("server busy"), stats, metrics CSV |
| `client.py` | Client: submits jobs on one connection, prints ACKs and results, handles "rejected", `get_stats()` helper |
| `test_p1.py` | D1 tests (concurrent clients, FAILED state, bad input, requeue) |
| `test_d2.py` | D2 tests (retry limit, overload/backpressure, recovery, stats) |
| `perf_p1.py` | D2 experiments + graphs (scalability, static vs dynamic worker failure) |

## Run (needs only Python 3; `pip3 install matplotlib` for graphs)
```
python3 test_p1.py        # 4 x PASS  (D1)
python3 test_d2.py        # 4 x PASS  (D2)
python3 perf_p1.py        # prints tables, writes results_*.csv and graph_*.png
```
Live demo: terminal 1 `python3 job_manager.py`, terminal 2/3 `python3 client.py 127.0.0.1 3 1.0 clientA`.
Ctrl+C on the job manager saves `metrics_job_manager.csv`.

## D2 rubric -> what to show
| D2 item | Marks | Person 1 evidence |
|---|---|---|
| Complete application functionality | 4 | concurrent clients, job IDs, 4 states, ACK + pushed results, retry limit (max 3 attempts), stats message |
| Challenging scenario / robustness | 4 | `test_d2.py` T2/T3: 25 jobs into a queue of 10 -> 15 rejected "server busy", server stays alive, recovers after draining. Also bad-input handling (test_p1 PASS 3) |
| Performance evaluation & comparison | 5 | `perf_p1.py`: latency / p95 / throughput for 1, 5, 10, 20 clients; static (no monitoring) vs dynamic (requeue) under a worker failure; graphs + CSVs |
| Dynamic SDN functionality | 6 | mostly Person 3; you provide `stats` (queue/running numbers) and `running_jobs_of()` + `requeue()` hooks |
| Documentation / viva | 6 | this file + your report section + screenshots of the PASS output and the graphs |

## Message protocol (newline-delimited JSON, TCP port 5001)
| Direction | Message |
|---|---|
| client -> server | `{"type":"submit_job","payload":{...}}` |
| server -> client | `{"type":"ack","job_id":"..","state":"QUEUED"}` |
| server -> client | `{"type":"rejected","reason":"server busy","queue_len":N}` |
| server -> client (pushed later) | `{"type":"result","job_id":"..","state":"COMPLETED"/"FAILED",...}` |
| client -> server | `{"type":"job_status","job_id":".."}` -> `{"type":"status",...}` |
| client -> server | `{"type":"stats"}` -> queued/running/completed/failed/rejected/avg_latency_s |

## Hooks for Person 2 / Person 3
```
store.next_queued(timeout)      get next waiting job (blocks)
store.mark_running(id, worker)
store.mark_completed(id, result)
store.mark_failed(id, reason)
store.running_jobs_of(worker)   all jobs on a worker  (call when it dies)
store.requeue(id)               back to QUEUED; becomes FAILED after max_attempts
store.full_stats()              numbers Person 3's controller can use for load decisions
```

## Honest note for the report
`perf_p1.py` uses simulated workers so you can run it alone. When Person 2's real workers and
Person 3's Mininet network work, rerun the same experiment against the real system and replace
the numbers/graphs. Say in the report which results are simulated and which are real.
