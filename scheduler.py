

class WorkerScheduler:
    def __init__(self):
        self.workers = {}
        self.last_index = -1

    def register_worker(self, worker_id):
        if worker_id not in self.workers:
            self.workers[worker_id] = "IDLE"
            print(f"Worker {worker_id} registered")

    def get_available_worker(self):
        worker_ids = list(self.workers.keys())
        count = len(worker_ids)

        if count == 0:
            return None

        for step in range(1, count + 1):
            index = (self.last_index + step) % count
            worker_id = worker_ids[index]

            if self.workers[worker_id] == "IDLE":
                self.last_index = index
                return worker_id

        return None

    def mark_busy(self, worker_id):
        if worker_id not in self.workers:
            raise ValueError("Unknown worker")

        if self.workers[worker_id] != "IDLE":
            raise ValueError("Worker is not idle")

        self.workers[worker_id] = "BUSY"

    def mark_idle(self, worker_id):
        if worker_id not in self.workers:
            raise ValueError("Unknown worker")

        self.workers[worker_id] = "IDLE"
