import time
import threading
from typing import Dict, List, Optional
import random
from queue import Queue

# Bus Details
class Bus:
    def __init__(self, bus_id: str, route: str, total_seats: int):
        self.bus_id = bus_id
        self.route = route
        self.total_seats = total_seats
        self.available_seats = total_seats
        self.bookings: Dict[str, List[int]] = {}  # user_id -> list of seat numbers
        self.lock = threading.Lock()  # Lock to prevent double booking

    def book_seats(self, user_id: str, num_seats: int) -> bool:
        with self.lock:  # Acquire lock to ensure atomicity
            if self.available_seats >= num_seats:
                self.available_seats -= num_seats
                if user_id not in self.bookings:
                    self.bookings[user_id] = []
                # Assign seat numbers
                seat_numbers = list(range(self.total_seats - self.available_seats - num_seats + 1, self.total_seats - self.available_seats + 1))
                self.bookings[user_id].extend(seat_numbers)
                return True
            return False

    def cancel_booking(self, user_id: str) -> bool:
        with self.lock:  # Acquire lock to ensure atomicity
            if user_id in self.bookings:
                self.available_seats += len(self.bookings[user_id])
                del self.bookings[user_id]
                return True
            return False

    def get_available_seats(self) -> int:
        with self.lock:  # Acquire lock to ensure atomicity
            return self.available_seats

# Bus Location Tracker
class BusLocationTracker:
    def __init__(self):
        self.bus_locations: Dict[str, str] = {}  # bus_id -> location
        self.lock = threading.Lock()  # Lock to ensure thread-safe location updates

    def update_location(self, bus_id: str, location: str):
        with self.lock:
            self.bus_locations[bus_id] = location

    def get_location(self, bus_id: str) -> Optional[str]:
        with self.lock:
            return self.bus_locations.get(bus_id)

# Ticket Booking Service
class TicketBookingService:
    def __init__(self):
        self.buses: Dict[str, Bus] = {}  # bus_id -> Bus
        self.location_tracker = BusLocationTracker()
        self.booking_queue = Queue()
        self.running = True

    def start(self):
        def process_bookings():
            while self.running:
                if not self.booking_queue.empty():
                    user_id, bus_id, num_seats = self.booking_queue.get()
                    self._book_seats(user_id, bus_id, num_seats)
                else:
                    time.sleep(0.1)  # Sleep briefly to avoid busy-waiting

        threading.Thread(target=process_bookings, daemon=True).start()

    def stop(self):
        self.running = False

    def add_bus(self, bus_id: str, route: str, total_seats: int):
        self.buses[bus_id] = Bus(bus_id, route, total_seats)

    def book_seats(self, user_id: str, bus_id: str, num_seats: int):
        self.booking_queue.put((user_id, bus_id, num_seats))

    def _book_seats(self, user_id: str, bus_id: str, num_seats: int):
        if bus_id in self.buses:
            bus = self.buses[bus_id]
            if bus.book_seats(user_id, num_seats):
                print(f"Booking successful: User {user_id} booked {num_seats} seats on Bus {bus_id}.")
            else:
                print(f"Booking failed: Not enough seats available on Bus {bus_id}.")
        else:
            print(f"Booking failed: Bus {bus_id} does not exist.")

    def cancel_booking(self, user_id: str, bus_id: str):
        if bus_id in self.buses:
            bus = self.buses[bus_id]
            if bus.cancel_booking(user_id):
                print(f"Cancellation successful: User {user_id} cancelled booking on Bus {bus_id}.")
            else:
                print(f"Cancellation failed: User {user_id} has no booking on Bus {bus_id}.")
        else:
            print(f"Cancellation failed: Bus {bus_id} does not exist.")

    def update_bus_location(self, bus_id: str, location: str):
        self.location_tracker.update_location(bus_id, location)
        print(f"Bus {bus_id} location updated to {location}.")

    def get_bus_location(self, bus_id: str) -> Optional[str]:
        return self.location_tracker.get_location(bus_id)

# Simulate User Activity
def simulate_user_activity(booking_service: TicketBookingService):
    user_ids = [f"user_{i}" for i in range(10)]
    bus_ids = ["bus_1", "bus_2"]
    while True:
        user_id = random.choice(user_ids)
        bus_id = random.choice(bus_ids)
        num_seats = random.randint(1, 5)
        booking_service.book_seats(user_id, bus_id, num_seats)
        time.sleep(1)  # Simulate activity every second

# Simulate Bus Location Updates
def simulate_bus_location_updates(booking_service: TicketBookingService):
    bus_ids = ["bus_1", "bus_2"]
    locations = ["Location A", "Location B", "Location C"]
    while True:
        bus_id = random.choice(bus_ids)
        location = random.choice(locations)
        booking_service.update_bus_location(bus_id, location)
        time.sleep(5)  # Simulate location updates every 5 seconds

# Main Function
def main():
    # Create ticket booking service
    booking_service = TicketBookingService()
    booking_service.add_bus("bus_1", "Route A", 50)
    booking_service.add_bus("bus_2", "Route B", 50)
    booking_service.start()

    # Simulate user activity in a separate thread
    threading.Thread(target=simulate_user_activity, args=(booking_service,), daemon=True).start()

    # Simulate bus location updates in a separate thread
    threading.Thread(target=simulate_bus_location_updates, args=(booking_service,), daemon=True).start()

    # Keep the main thread alive to allow background processing
    time.sleep(20)
    booking_service.stop()

if __name__ == "__main__":
    main()