import unittest
from unittest.mock import MagicMock
from types import SimpleNamespace

from worker import process_job


class TestWorkerIntegration(unittest.TestCase):

    def test_sum_job(self):
        job = {
            "job_id": "1",
            "operation": "SUM",
            "start": 1,
            "end": 10
        }

        result = process_job(job)

        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["result"], 55)

    def test_multiply_job(self):
        job = {
            "job_id": "2",
            "operation": "MULTIPLY",
            "a": 5,
            "b": 4
        }

        result = process_job(job)

        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["result"], 20)

    def test_invalid_job(self):
        job = {
            "job_id": "3",
            "operation": "UNKNOWN"
        }

        result = process_job(job)

        self.assertEqual(result["status"], "FAILED")

    def test_worker_manager_initialization(self):
        from worker_manager import WorkerManager

        store = MagicMock()
        manager = WorkerManager(store, number_of_workers=2)

        self.assertEqual(len(manager.scheduler.workers), 2)
        self.assertIn("worker1", manager.scheduler.workers)
        self.assertIn("worker2", manager.scheduler.workers)

    def test_round_robin_assignment(self):
        from worker_manager import WorkerManager

        store = MagicMock()
        manager = WorkerManager(store, number_of_workers=2)

        first = manager.scheduler.get_available_worker()
        manager.scheduler.mark_busy(first)

        second = manager.scheduler.get_available_worker()

        self.assertNotEqual(first, second)



    def test_failed_job(self):
        job = {
            "job_id": "4",
            "operation": "SUM",
            "start": 20,
            "end": 10
        }

        result = process_job(job)
        self.assertEqual(result["status"], "FAILED")
        self.assertIn("start must not exceed end", result["error"])

        if __name__ == "__main__":
           unittest.main()
