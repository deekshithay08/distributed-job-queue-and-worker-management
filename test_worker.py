
import unittest
from worker import execute_job, process_job
from scheduler import WorkerScheduler


class TestWorker(unittest.TestCase):

    def test_sum(self):
        job = {
            "job_id": 1,
            "operation": "SUM",
            "start": 1,
            "end": 100
        }
        self.assertEqual(execute_job(job), 5050)

    def test_multiply(self):
        job = {
            "job_id": 2,
            "operation": "MULTIPLY",
            "a": 6,
            "b": 7
        }
        self.assertEqual(execute_job(job), 42)

    def test_invalid_operation(self):
        job = {"job_id": 3, "operation": "UNKNOWN"}

        with self.assertRaises(ValueError):
            execute_job(job)

    def test_invalid_sum_range(self):
        job = {
            "job_id": 4,
            "operation": "SUM",
            "start": 10,
            "end": 1
        }

        with self.assertRaises(ValueError):
            execute_job(job)

    def test_process_job_success(self):
        job = {
            "job_id": 5,
            "operation": "MULTIPLY",
            "a": 3,
            "b": 4
        }

        result = process_job(job)
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["result"], 12)

    def test_process_job_failure(self):
        job = {"job_id": 6, "operation": "UNKNOWN"}

        result = process_job(job)
        self.assertEqual(result["status"], "FAILED")
        self.assertIn("error", result)

    def test_scheduler_round_robin(self):
        scheduler = WorkerScheduler()
        scheduler.register_worker("worker1")
        scheduler.register_worker("worker2")

        self.assertEqual(
            scheduler.get_available_worker(), "worker1"
        )
        self.assertEqual(
            scheduler.get_available_worker(), "worker2"
        )
        self.assertEqual(
            scheduler.get_available_worker(), "worker1"
        )

    def test_scheduler_busy_and_idle(self):
        scheduler = WorkerScheduler()
        scheduler.register_worker("worker1")

        scheduler.mark_busy("worker1")
        self.assertEqual(scheduler.workers["worker1"], "BUSY")
        self.assertIsNone(scheduler.get_available_worker())

        scheduler.mark_idle("worker1")
        self.assertEqual(scheduler.workers["worker1"], "IDLE")
        self.assertEqual(
            scheduler.get_available_worker(), "worker1"
        )


if __name__ == "__main__":
    unittest.main()
