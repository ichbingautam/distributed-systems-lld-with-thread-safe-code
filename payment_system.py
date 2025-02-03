import time
import uuid
from abc import ABC, abstractmethod
from typing import Optional, Dict

# Payment Gateway Interface
class PaymentGateway(ABC):
    @abstractmethod
    def charge(self, amount: float, currency: str, idempotency_key: str) -> bool:
        pass

    @abstractmethod
    def refund(self, transaction_id: str, amount: float) -> bool:
        pass

# Mock Payment Gateway (Simulates external payment gateway)
class MockPaymentGateway(PaymentGateway):
    def __init__(self):
        self.transactions: Dict[str, str] = {}  # Maps idempotency_key to transaction_id

    def charge(self, amount: float, currency: str, idempotency_key: str) -> bool:
        # Simulate network failure 20% of the time
        if time.time() % 5 < 1:
            raise Exception("Network failure during payment processing.")

        # Ensure idempotency
        if idempotency_key in self.transactions:
            print(f"Duplicate request detected. Returning existing transaction for idempotency key: {idempotency_key}")
            return True

        # Simulate payment processing
        transaction_id = str(uuid.uuid4())
        self.transactions[idempotency_key] = transaction_id
        print(f"Payment processed. Transaction ID: {transaction_id}")
        return True

    def refund(self, transaction_id: str, amount: float) -> bool:
        # Simulate refund processing
        print(f"Refund processed for Transaction ID: {transaction_id}, Amount: ${amount:.2f}")
        return True

# Payment Processor
class PaymentProcessor:
    def __init__(self, payment_gateway: PaymentGateway):
        self.payment_gateway = payment_gateway

    def process_payment(self, amount: float, currency: str, idempotency_key: str, max_retries: int = 3) -> Optional[str]:
        retries = 0
        while retries < max_retries:
            try:
                if self.payment_gateway.charge(amount, currency, idempotency_key):
                    return self.payment_gateway.transactions[idempotency_key]
            except Exception as e:
                print(f"Payment failed (Attempt {retries + 1}): {e}")
                retries += 1
                time.sleep(2 ** retries)  # Exponential backoff
        print("Payment processing failed after maximum retries.")
        return None

# Refund Manager
class RefundManager:
    def __init__(self, payment_gateway: PaymentGateway):
        self.payment_gateway = payment_gateway

    def process_refund(self, transaction_id: str, amount: float) -> bool:
        return self.payment_gateway.refund(transaction_id, amount)

# Payment Service
class PaymentService:
    def __init__(self):
        self.payment_gateway = MockPaymentGateway()
        self.payment_processor = PaymentProcessor(self.payment_gateway)
        self.refund_manager = RefundManager(self.payment_gateway)

    def make_payment(self, user_id: str, amount: float, currency: str) -> Optional[str]:
        # Generate a unique idempotency key
        idempotency_key = f"{user_id}_{int(time.time())}"

        # Process payment
        transaction_id = self.payment_processor.process_payment(amount, currency, idempotency_key)
        return transaction_id

    def refund_payment(self, transaction_id: str, amount: float) -> bool:
        return self.refund_manager.process_refund(transaction_id, amount)

# Simulate Payment Workflow
def simulate_payment_workflow():
    payment_service = PaymentService()

    # Simulate a payment
    user_id = "user_123"
    amount = 100.0
    currency = "USD"

    # Simulate duplicate payment requests
    for _ in range(3):
        transaction_id = payment_service.make_payment(user_id, amount, currency)
        if transaction_id:
            print(f"Payment successful. Transaction ID: {transaction_id}")
        else:
            print("Payment failed.")

    # Simulate a refund
    if transaction_id:
        time.sleep(2)
        if payment_service.refund_payment(transaction_id, amount):
            print("Refund successful.")
        else:
            print("Refund failed.")

# Main Function
def main():
    simulate_payment_workflow()

if __name__ == "__main__":
    main()