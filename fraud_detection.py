import time
import threading
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import random
from queue import Queue

# Fraud Types
class FraudType:
    FAKE_ACCOUNT = "fake_account"
    PAYMENT_FRAUD = "payment_fraud"
    RIDE_ABUSE = "ride_abuse"

# Fraud Detection Strategy Interface
class FraudDetectionStrategy(ABC):
    @abstractmethod
    def detect_fraud(self, data: Dict) -> Optional[str]:
        pass

# Rule-Based Fraud Detection
class RuleBasedDetection(FraudDetectionStrategy):
    def detect_fraud(self, data: Dict) -> Optional[str]:
        # Rule 1: Detect fake accounts (multiple accounts from the same IP)
        if data.get("ip_address") and data.get("num_accounts_from_ip", 0) > 3:
            return FraudType.FAKE_ACCOUNT

        # Rule 2: Detect payment fraud (high-value transactions)
        if data.get("transaction_amount", 0) > 1000:
            return FraudType.PAYMENT_FRAUD

        # Rule 3: Detect ride abuse (too many cancellations)
        if data.get("ride_cancellations", 0) > 5:
            return FraudType.RIDE_ABUSE

        return None

# Machine Learning Fraud Detection
class MachineLearningDetection(FraudDetectionStrategy):
    def __init__(self):
        # Simulate a trained ML model
        self.model = self._train_model()

    def _train_model(self):
        # Simulate training a model (e.g., using historical fraud data)
        print("Training ML model...")
        time.sleep(2)  # Simulate training time
        return "trained_model"

    def detect_fraud(self, data: Dict) -> Optional[str]:
        # Simulate ML-based fraud detection
        if random.random() < 0.1:  # 10% chance of detecting fraud
            return random.choice([FraudType.FAKE_ACCOUNT, FraudType.PAYMENT_FRAUD, FraudType.RIDE_ABUSE])
        return None

# Fraud Detection Service
class FraudDetectionService:
    def __init__(self):
        self.rule_based_detector = RuleBasedDetection()
        self.ml_detector = MachineLearningDetection()
        self.data_queue = Queue()
        self.running = True

    def start(self):
        def process_data():
            while self.running:
                if not self.data_queue.empty():
                    data = self.data_queue.get()
                    self._detect_fraud(data)
                else:
                    time.sleep(0.1)  # Sleep briefly to avoid busy-waiting

        threading.Thread(target=process_data, daemon=True).start()

    def stop(self):
        self.running = False

    def add_data(self, data: Dict):
        self.data_queue.put(data)

    def _detect_fraud(self, data: Dict):
        # Use rule-based detection for real-time checks
        fraud_type = self.rule_based_detector.detect_fraud(data)
        if fraud_type:
            print(f"Rule-based detection: Fraud detected - {fraud_type}")
        else:
            # Use ML-based detection for offline analysis
            fraud_type = self.ml_detector.detect_fraud(data)
            if fraud_type:
                print(f"ML-based detection: Fraud detected - {fraud_type}")

# Simulate Data Generation
def simulate_data_generation(fraud_detection_service: FraudDetectionService):
    while True:
        data = {
            "ip_address": f"192.168.1.{random.randint(1, 10)}",
            "num_accounts_from_ip": random.randint(1, 5),
            "transaction_amount": random.randint(100, 2000),
            "ride_cancellations": random.randint(0, 10),
        }
        fraud_detection_service.add_data(data)
        time.sleep(1)  # Simulate data generation every second

# Main Function
def main():
    # Create fraud detection service
    fraud_detection_service = FraudDetectionService()
    fraud_detection_service.start()

    # Simulate data generation in a separate thread
    threading.Thread(target=simulate_data_generation, args=(fraud_detection_service,), daemon=True).start()

    # Keep the main thread alive to allow background processing
    time.sleep(20)
    fraud_detection_service.stop()

if __name__ == "__main__":
    main()