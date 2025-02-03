import time
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from queue import Queue
from typing import Dict, List, Optional
import random
import json
import zlib

# Location Data
class Location:
    def __init__(self, latitude: float, longitude: float):
        self.latitude = latitude
        self.longitude = longitude
        self.timestamp = datetime.now()

    def __str__(self):
        return f"({self.latitude}, {self.longitude}) at {self.timestamp}"

# Location Storage Interface
class LocationStorage(ABC):
    @abstractmethod
    def store_location(self, driver_id: str, location: Location):
        pass

    @abstractmethod
    def get_latest_location(self, driver_id: str) -> Optional[Location]:
        pass

# Optimized GPS Data Storage (Store only significant changes and compress data)
class OptimizedLocationStorage(LocationStorage):
    def __init__(self):
        self.locations: Dict[str, List[bytes]] = {}  # Store compressed location data
        self.lock = threading.Lock()

    def store_location(self, driver_id: str, location: Location):
        with self.lock:
            if driver_id not in self.locations:
                self.locations[driver_id] = []
            # Store only if the location has changed significantly
            if not self.locations[driver_id] or self._is_significant_change(self._decompress(self.locations[driver_id][-1]), location):
                compressed_data = self._compress(location)
                self.locations[driver_id].append(compressed_data)
                print(f"Stored location for driver {driver_id}: {location}")

    def get_latest_location(self, driver_id: str) -> Optional[Location]:
        with self.lock:
            if driver_id in self.locations and self.locations[driver_id]:
                return self._decompress(self.locations[driver_id][-1])
            return None

    def _is_significant_change(self, prev_location: Location, new_location: Location) -> bool:
        # Define a threshold for significant change (e.g., 10 meters)
        THRESHOLD = 0.0001  # Approx 10 meters in latitude/longitude
        return (abs(prev_location.latitude - new_location.latitude) > THRESHOLD or
                abs(prev_location.longitude - new_location.longitude) > THRESHOLD)

    def _compress(self, location: Location) -> bytes:
        # Compress location data using zlib
        data = json.dumps({"lat": location.latitude, "lon": location.longitude, "ts": location.timestamp.isoformat()})
        return zlib.compress(data.encode())

    def _decompress(self, compressed_data: bytes) -> Location:
        # Decompress location data using zlib
        data = zlib.decompress(compressed_data).decode()
        location_data = json.loads(data)
        return Location(location_data["lat"], location_data["lon"])

# Real-Time Location Tracker
class LocationTracker:
    def __init__(self, location_storage: LocationStorage):
        self.location_storage = location_storage
        self.location_queue = Queue()
        self.running = True

    def start(self):
        def process_locations():
            while self.running:
                if not self.location_queue.empty():
                    driver_id, location = self.location_queue.get()
                    self.location_storage.store_location(driver_id, location)
                else:
                    time.sleep(0.1)  # Sleep briefly to avoid busy-waiting

        threading.Thread(target=process_locations, daemon=True).start()

    def stop(self):
        self.running = False

    def update_location(self, driver_id: str, location: Location):
        self.location_queue.put((driver_id, location))

# Driver Simulator (Simulates a driver sending GPS updates)
class DriverSimulator:
    def __init__(self, driver_id: str, location_tracker: LocationTracker):
        self.driver_id = driver_id
        self.location_tracker = location_tracker
        self.local_cache: List[Location] = []  # Local cache for low-network areas

    def start(self):
        def simulate_driver():
            latitude, longitude = 37.7749, -122.4194  # Starting location (San Francisco)
            while True:
                # Simulate small GPS updates
                latitude += random.uniform(-0.0001, 0.0001)
                longitude += random.uniform(-0.0001, 0.0001)
                location = Location(latitude, longitude)

                # Simulate low-network conditions (e.g., 20% chance of network failure)
                if random.random() < 0.2:
                    self.local_cache.append(location)  # Cache location locally
                    print(f"Driver {self.driver_id} is offline. Caching location: {location}")
                else:
                    # Sync cached locations first
                    while self.local_cache:
                        cached_location = self.local_cache.pop(0)
                        self.location_tracker.update_location(self.driver_id, cached_location)
                    # Send current location
                    self.location_tracker.update_location(self.driver_id, location)

                time.sleep(1)  # Send updates every second

        threading.Thread(target=simulate_driver, daemon=True).start()

# Rider Simulator (Simulates a rider receiving real-time updates)
class RiderSimulator:
    def __init__(self, rider_id: str, driver_id: str, location_storage: LocationStorage):
        self.rider_id = rider_id
        self.driver_id = driver_id
        self.location_storage = location_storage

    def start(self):
        def simulate_rider():
            while True:
                location = self.location_storage.get_latest_location(self.driver_id)
                if location:
                    print(f"Rider {self.rider_id} received update: Driver {self.driver_id} is at {location}")
                else:
                    print(f"Rider {self.rider_id} has no updates for Driver {self.driver_id}.")
                time.sleep(2)  # Check for updates every 2 seconds

        threading.Thread(target=simulate_rider, daemon=True).start()

# Simulate Drivers and Riders
def simulate_system():
    location_storage = OptimizedLocationStorage()
    location_tracker = LocationTracker(location_storage)
    location_tracker.start()

    # Simulate drivers
    drivers = [DriverSimulator(f"driver_{i}", location_tracker) for i in range(3)]
    for driver in drivers:
        driver.start()

    # Simulate riders
    riders = [RiderSimulator(f"rider_{i}", f"driver_{i}", location_storage) for i in range(3)]
    for rider in riders:
        rider.start()

    # Keep the main thread alive to allow background processing
    time.sleep(20)
    location_tracker.stop()

# Main Function
def main():
    simulate_system()

if __name__ == "__main__":
    main()