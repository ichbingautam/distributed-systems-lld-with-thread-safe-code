import time
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from queue import Queue
from typing import List, Dict
import random

# Log Entry
class LogEntry:
    def __init__(self, service_name: str, log_level: str, message: str):
        self.service_name = service_name
        self.log_level = log_level
        self.message = message
        self.timestamp = datetime.now()

    def __str__(self):
        return f"[{self.timestamp}] [{self.service_name}] [{self.log_level}] {self.message}"

# Log Storage Interface
class LogStorage(ABC):
    @abstractmethod
    def store_log(self, log_entry: LogEntry):
        pass

    @abstractmethod
    def delete_old_logs(self, retention_days: int):
        pass

# Distributed Log Storage (Simulates Elasticsearch/S3)
class DistributedLogStorage(LogStorage):
    def __init__(self):
        self.logs: List[LogEntry] = []
        self.lock = threading.Lock()

    def store_log(self, log_entry: LogEntry):
        with self.lock:
            self.logs.append(log_entry)
            print(f"Stored log: {log_entry}")

    def delete_old_logs(self, retention_days: int):
        with self.lock:
            cutoff_time = datetime.now() - timedelta(days=retention_days)
            self.logs = [log for log in self.logs if log.timestamp >= cutoff_time]
            print(f"Deleted logs older than {retention_days} days.")

# Log Aggregator (Simulates Kafka/RabbitMQ)
class LogAggregator:
    def __init__(self, log_storage: LogStorage):
        self.log_storage = log_storage
        self.log_queue = Queue()
        self.running = True

    def start(self):
        def process_logs():
            batch = []
            while self.running:
                if not self.log_queue.empty():
                    log_entry = self.log_queue.get()
                    batch.append(log_entry)
                    if len(batch) >= 10:  # Batch size of 10
                        self.log_storage.store_log(batch)
                        batch = []
                else:
                    time.sleep(0.1)  # Sleep briefly to avoid busy-waiting

        threading.Thread(target=process_logs, daemon=True).start()

    def stop(self):
        self.running = False

    def add_log(self, log_entry: LogEntry):
        self.log_queue.put(log_entry)

# Log Retention Manager
class LogRetentionManager:
    def __init__(self, log_storage: LogStorage, retention_days: int):
        self.log_storage = log_storage
        self.retention_days = retention_days

    def enforce_retention_policy(self):
        while True:
            self.log_storage.delete_old_logs(self.retention_days)
            time.sleep(86400)  # Run once per day

# Log Producer (Simulates a microservice generating logs)
class LogProducer:
    def __init__(self, service_name: str, log_aggregator: LogAggregator):
        self.service_name = service_name
        self.log_aggregator = log_aggregator

    def log(self, log_level: str, message: str):
        log_entry = LogEntry(self.service_name, log_level, message)
        self.log_aggregator.add_log(log_entry)

# Simulate Microservices and Logging
def simulate_microservices():
    log_storage = DistributedLogStorage()
    log_aggregator = LogAggregator(log_storage)
    log_aggregator.start()

    # Start log retention manager
    retention_manager = LogRetentionManager(log_storage, retention_days=1)
    threading.Thread(target=retention_manager.enforce_retention_policy, daemon=True).start()

    # Simulate multiple microservices generating logs
    services = [
        LogProducer("auth_service", log_aggregator),
        LogProducer("payment_service", log_aggregator),
        LogProducer("ride_service", log_aggregator),
    ]

    for i in range(100):  # Simulate 100 log messages
        for service in services:
            service.log("INFO", f"Log message {i} from {service.service_name}")
        time.sleep(random.uniform(0.01, 0.1))  # Simulate variable load

    # Keep the main thread alive to allow background processing
    time.sleep(10)
    log_aggregator.stop()

# Main Function
def main():
    simulate_microservices()

if __name__ == "__main__":
    main()