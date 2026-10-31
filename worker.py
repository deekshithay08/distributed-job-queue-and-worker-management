
import time


def execute_job(job):
    operation = job.get("operation")

    if operation == "SUM":
        start = job.get("start")
        end = job.get("end")

        if type(start) is not int or type(end) is not int:
            raise ValueError("start and end must be integers")
        if start > end:
            raise ValueError("start must not exceed end")

        return sum(range(start, end + 1))

    elif operation == "MULTIPLY":
        a = job.get("a")
        b = job.get("b")

        if not isinstance(a, (int, float)) or isinstance(a, bool):
            raise ValueError("a must be a number")
        if not isinstance(b, (int, float)) or isinstance(b, bool):
            raise ValueError("b must be a number")

        return a * b

    elif "duration" in job:
        duration = job.get("duration")

        if not isinstance(duration, (int, float)) or isinstance(duration, bool):
            raise ValueError("duration must be a number")
        if duration < 0:
            raise ValueError("duration cannot be negative")

        time.sleep(duration)

        return {
            "client": job.get("client"),
            "seq": job.get("seq"),
            "message": "Job executed successfully"
        }

    raise ValueError("Unknown operation")


def process_job(job):
    try:
        result = execute_job(job)
        return {
            "job_id": job.get("job_id"),
            "status": "COMPLETED",
            "result": result
        }
    except (ValueError, TypeError) as error:
        return {
            "job_id": job.get("job_id"),
            "status": "FAILED",
            "error": str(error)
        }


if __name__ == "__main__":
    job = {
        "job_id": 1,
        "operation": "SUM",
        "start": 1,
        "end": 100
    }
    print(process_job(job))
