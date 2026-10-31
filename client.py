"""
PERSON 1 - Client.

Usage: python3 client.py <server_ip> [num_jobs] [duration_s] [client_name]

Opens ONE persistent TCP connection, submits num_jobs jobs, prints each ack
immediately, then prints each result as the server pushes it.
D2: if the server is overloaded it answers "rejected"; the client counts that
job as rejected (it does not wait for a result that will never come).
"""
import json
import socket
import sys
import time

PORT = 5001


def run_client(server_ip, num_jobs=3, duration=1.0, name="client", port=PORT,
               quiet=False, result_timeout=30):
    """Returns (acks, results, rejected_count). `results` have a 'client_latency_s' field."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_ip, port))
    f = sock.makefile("r")
    submit_time, acks, results, rejected = {}, [], [], 0

    for i in range(num_jobs):
        msg = {"type": "submit_job", "payload": {"duration": duration, "client": name, "seq": i}}
        t0 = time.time()
        sock.sendall((json.dumps(msg) + "\n").encode())
        ack = json.loads(f.readline())
        if ack["type"] == "rejected":
            rejected += 1
            if not quiet:
                print(f"[{name}] REJECTED ({ack['reason']})")
            continue
        if ack["type"] != "ack":
            raise RuntimeError(f"expected ack, got {ack}")
        submit_time[ack["job_id"]] = t0
        acks.append(ack)
        if not quiet:
            print(f"[{name}] ACK  job={ack['job_id']} state={ack['state']} ({(time.time()-t0)*1000:.1f} ms)")

    sock.settimeout(result_timeout)
    try:
        while len(results) < len(acks):
            line = f.readline()
            if not line:
                break
            r = json.loads(line)
            if r["type"] == "result":
                r["client_latency_s"] = time.time() - submit_time[r["job_id"]]
                results.append(r)
                if not quiet:
                    print(f"[{name}] DONE job={r['job_id']} state={r['state']} "
                          f"latency={r['client_latency_s']:.2f}s")
    except (socket.timeout, OSError):
        pass          # some results never came back (e.g. worker died, no recovery)
    sock.close()
    return acks, results, rejected


def get_stats(server_ip, port=PORT):
    """Ask the job manager for its current stats (queue length, rejected, ...)."""
    s = socket.create_connection((server_ip, port), timeout=5)
    s.sendall(b'{"type":"stats"}\n')
    reply = json.loads(s.makefile("r").readline())
    s.close()
    return reply


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    run_client(sys.argv[1],
               int(sys.argv[2]) if len(sys.argv) > 2 else 3,
               float(sys.argv[3]) if len(sys.argv) > 3 else 1.0,
               sys.argv[4] if len(sys.argv) > 4 else "client")
