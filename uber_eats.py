import threading
import time
import random
from queue import Queue
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

# Constants
ORDER_PREPARATION_TIME = 5  # in seconds
MAX_DELIVERY_TIME = 30  # in seconds

# Shared Queue for Orders
order_queue = Queue()

# Lock for thread-safe operations
print_lock = threading.Lock()

# Abstract Class for Order State
class OrderState(ABC):
    @abstractmethod
    def update_status(self, order):
        pass

class PendingState(OrderState):
    def update_status(self, order):
        order.status = "Pending"
        with print_lock:
            print(f"Order {order.order_id} is now Pending.")

class PreparedState(OrderState):
    def update_status(self, order):
        order.status = "Prepared"
        with print_lock:
            print(f"Order {order.order_id} is now Prepared.")

class DeliveredState(OrderState):
    def update_status(self, order):
        order.status = "Delivered"
        with print_lock:
            print(f"Order {order.order_id} is now Delivered.")

class CancelledState(OrderState):
    def update_status(self, order):
        order.status = "Cancelled"
        with print_lock:
            print(f"Order {order.order_id} is now Cancelled.")

# Order Class
class Order:
    def __init__(self, order_id, items, user_id):
        self.order_id = order_id
        self.items = items
        self.user_id = user_id
        self.status = PendingState()
        self.eta = None
        self.created_at = datetime.now()

    def update_status(self, state: OrderState):
        state.update_status(self)

    def set_eta(self, eta):
        self.eta = eta
        with print_lock:
            print(f"Order {self.order_id} ETA: {eta}")

    def cancel(self):
        if isinstance(self.status, PendingState):
            self.update_status(CancelledState())
        else:
            with print_lock:
                print(f"Order {self.order_id} cannot be cancelled. Current status: {self.status}")

# Abstract Class for Service Providers
class ServiceProvider(ABC):
    @abstractmethod
    def process_order(self, order):
        pass

# Restaurant Class
class Restaurant(ServiceProvider):
    def __init__(self, name):
        self.name = name

    def process_order(self, order):
        with print_lock:
            print(f"Restaurant {self.name} is preparing Order {order.order_id}...")
        time.sleep(ORDER_PREPARATION_TIME)  # Simulate preparation time
        order.update_status(PreparedState())

# Delivery Partner Class
class DeliveryPartner(ServiceProvider):
    def __init__(self, partner_id):
        self.partner_id = partner_id

    def process_order(self, order):
        with print_lock:
            print(f"Delivery Partner {self.partner_id} is delivering Order {order.order_id}...")
        delivery_time = random.randint(5, MAX_DELIVERY_TIME)  # Simulate delivery time
        time.sleep(delivery_time)
        order.update_status(DeliveredState())
        order.set_eta(order.created_at + timedelta(seconds=delivery_time + ORDER_PREPARATION_TIME))

# Order Processor
class OrderProcessor:
    def __init__(self):
        self.restaurant = Restaurant("Foodie's Place")
        self.delivery_partners = [DeliveryPartner(i) for i in range(1, 101)]  # 100 delivery partners

    def process_orders(self):
        while True:
            if not order_queue.empty():
                order = order_queue.get()
                self.restaurant.process_order(order)
                delivery_partner = random.choice(self.delivery_partners)
                delivery_partner.process_order(order)
            else:
                time.sleep(1)  # Wait for new orders

# Order Manager
class OrderManager:
    @staticmethod
    def place_order(items, user_id):
        order_id = random.randint(1000, 9999)
        order = Order(order_id, items, user_id)
        with print_lock:
            print(f"Order {order.order_id} placed by User {user_id}.")
        order_queue.put(order)
        return order

# Simulate user actions
def simulate_user_actions():
    # User places an order
    order = OrderManager.place_order(["Pizza", "Burger"], user_id=1)

    # Simulate a cancellation request after 2 seconds
    time.sleep(2)
    order.cancel()

    # User places another order
    order2 = OrderManager.place_order(["Pasta", "Salad"], user_id=2)
    time.sleep(2)

# Main Function
def main():
    # Start order processing thread
    order_processor = OrderProcessor()
    order_processor_thread = threading.Thread(target=order_processor.process_orders, daemon=True)
    order_processor_thread.start()

    # Simulate user actions
    simulate_user_actions()

    # Keep the main thread alive to allow background processing
    time.sleep(20)

if __name__ == "__main__":
    main()