import time
import threading
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import random
from queue import Queue, PriorityQueue

# Notification Types
class NotificationType:
    RIDE_CONFIRMATION = "ride_confirmation"
    SURGE_PRICING = "surge_pricing"
    PROMOTIONAL = "promotional"

# Notification Priority Levels
class NotificationPriority:
    HIGH = 1  # High-priority notifications (e.g., ride confirmation)
    MEDIUM = 2  # Medium-priority notifications (e.g., surge pricing)
    LOW = 3  # Low-priority notifications (e.g., promotional offers)

# Notification
class Notification:
    def __init__(self, user_id: str, message: str, notification_type: str, priority: int):
        self.user_id = user_id
        self.message = message
        self.notification_type = notification_type
        self.priority = priority
        self.timestamp = time.time()

    def __lt__(self, other):
        # PriorityQueue uses this method to order notifications
        return self.priority < other.priority

# Notification Channel Interface
class NotificationChannel(ABC):
    @abstractmethod
    def send(self, notification: Notification) -> bool:
        pass

# SMS Notification Channel
class SMSNotificationChannel(NotificationChannel):
    def send(self, notification: Notification) -> bool:
        # Simulate sending an SMS (90% success rate)
        success = random.random() < 0.9
        print(f"Sending SMS to User {notification.user_id}: {notification.message}")
        return success

# Email Notification Channel
class EmailNotificationChannel(NotificationChannel):
    def send(self, notification: Notification) -> bool:
        # Simulate sending an email (95% success rate)
        success = random.random() < 0.95
        print(f"Sending Email to User {notification.user_id}: {notification.message}")
        return success

# Push Notification Channel
class PushNotificationChannel(NotificationChannel):
    def send(self, notification: Notification) -> bool:
        # Simulate sending a push notification (85% success rate)
        success = random.random() < 0.85
        print(f"Sending Push Notification to User {notification.user_id}: {notification.message}")
        return success

# Notification Service
class NotificationService:
    def __init__(self):
        self.notification_queue = PriorityQueue()  # Priority queue for notifications
        self.channels = [
            SMSNotificationChannel(),
            EmailNotificationChannel(),
            PushNotificationChannel(),
        ]
        self.max_retries = 3
        self.running = True

    def start(self):
        def process_notifications():
            while self.running:
                if not self.notification_queue.empty():
                    notification = self.notification_queue.get()
                    self._send_notification(notification)
                else:
                    time.sleep(0.1)  # Sleep briefly to avoid busy-waiting

        threading.Thread(target=process_notifications, daemon=True).start()

    def stop(self):
        self.running = False

    def send_notification(self, user_id: str, message: str, notification_type: str, priority: int):
        notification = Notification(user_id, message, notification_type, priority)
        self.notification_queue.put(notification)

    def _send_notification(self, notification: Notification):
        for channel in self.channels:
            retries = 0
            while retries < self.max_retries:
                if channel.send(notification):
                    break  # Notification sent successfully
                retries += 1
                print(f"Retrying ({retries}/{self.max_retries}) for User {notification.user_id}")
                time.sleep(2 ** retries)  # Exponential backoff
            else:
                print(f"Failed to send notification to User {notification.user_id} via {channel.__class__.__name__}")

# Simulate User Activity
def simulate_user_activity(notification_service: NotificationService):
    user_ids = [f"user_{i}" for i in range(10)]
    while True:
        user_id = random.choice(user_ids)
        notification_type = random.choice([
            NotificationType.RIDE_CONFIRMATION,
            NotificationType.SURGE_PRICING,
            NotificationType.PROMOTIONAL,
        ])
        priority = (
            NotificationPriority.HIGH if notification_type == NotificationType.RIDE_CONFIRMATION
            else NotificationPriority.MEDIUM if notification_type == NotificationType.SURGE_PRICING
            else NotificationPriority.LOW
        )
        message = (
            f"Your ride is confirmed!" if notification_type == NotificationType.RIDE_CONFIRMATION
            else "Surge pricing is active!" if notification_type == NotificationType.SURGE_PRICING
            else "Check out our latest promotions!"
        )
        notification_service.send_notification(user_id, message, notification_type, priority)
        time.sleep(1)  # Simulate activity every second

# Main Function
def main():
    # Create notification service
    notification_service = NotificationService()
    notification_service.start()

    # Simulate user activity in a separate thread
    threading.Thread(target=simulate_user_activity, args=(notification_service,), daemon=True).start()

    # Keep the main thread alive to allow background processing
    time.sleep(20)
    notification_service.stop()

if __name__ == "__main__":
    main()