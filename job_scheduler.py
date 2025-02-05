import time
import threading
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import heapq
from queue import Queue
import random

# Job Class
class Job:
    def __init__(self, job_id: str, execution_time: float, retries: int = 3):
        self.job_id = job_id
        self.execution_time = execution_time
        self.retries = retries

    def __lt__(self, other):
        # Priority queue uses this method to order jobs
        return self.execution_time < other.execution_time

# Job Executor Interface
class JobExecutor(ABC):
    @abstractmethod
    def execute(self, job: Job) -> bool:
        pass

# Simulated Job Executor
class SimulatedJobExecutor(JobExecutor):
    def execute(self, job: Job) -> bool:
        # Simulate job execution (70% success rate)
        success = random.random() < 0.7
        print(f"Executing Job {job.job_id}... {'Success' if success else 'Failed'}")
        return success

# Job Scheduler
class JobScheduler:
    def __init__(self, job_executor: JobExecutor):
        self.job_executor = job_executor
        self.job_queue = []
        self.lock = threading.Lock()
        self.running = True

    def start(self):
        def process_jobs():
            while self.running:
                with self.lock:
                    if self.job_queue and self.job_queue[0].execution_time <= time.time():
                        job = heapq.heappop(self.job_queue)
                        if not self.job_executor.execute(job):
                            if job.retries > 0:
                                job.retries -= 1
                                job.execution_time = time.time() + 5  # Retry after 5 seconds
                                heapq.heappush(self.job_queue, job)
                                print(f"Retrying Job {job.job_id} in 5 seconds...")
                        else:
                            print(f"Job {job.job_id} completed successfully.")
                time.sleep(0.1)  # Sleep briefly to avoid busy-waiting

        threading.Thread(target=process_jobs, daemon=True).start()

    def stop(self):
        self.running = False

    def schedule_job(self, job: Job):
        with self.lock:
            heapq.heappush(self.job_queue, job)
            print(f"Scheduled Job {job.job_id} for execution at {job.execution_time}.")

# Simulate Job Scheduling
def simulate_job_scheduling(job_scheduler: JobScheduler):
    for i in range(10):  # Simulate 10 jobs
        job_id = f"job_{i}"
        execution_time = time.time() + random.randint(1, 10)  # Schedule jobs 1-10 seconds in the future
        job = Job(job_id, execution_time)
        job_scheduler.schedule_job(job)
        time.sleep(1)  # Simulate job creation every second

# Main Function
def main():
    # Create job executor and scheduler
    job_executor = SimulatedJobExecutor()
    job_scheduler = JobScheduler(job_executor)
    job_scheduler.start()

    # Simulate job scheduling in a separate thread
    threading.Thread(target=simulate_job_scheduling, args=(job_scheduler,), daemon=True).start()

    # Keep the main thread alive to allow background processing
    time.sleep(20)
    job_scheduler.stop()

if __name__ == "__main__":
    main()