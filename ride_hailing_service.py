import threading
import time
import random
from queue import Queue
from datetime import datetime
from abc import ABC, abstractmethod
from math import radians, sin, cos, sqrt, atan2

# Constants
EARTH_RADIUS_KM = 6371.0
BASE_FARE = 5.0
FARE_PER_KM = 2.0
FARE_PER_MINUTE = 0.5

# Shared Queue for Ride Requests
ride_request_queue = Queue()

# Lock for thread-safe operations
print_lock = threading.Lock()

# Abstract Class for Pricing Strategy
class PricingStrategy(ABC):
    @abstractmethod
    def calculate_fare(self, distance_km, duration_min):
        pass

# Base Pricing Strategy
class BasePricing(PricingStrategy):
    def calculate_fare(self, distance_km, duration_min):
        return BASE_FARE + (FARE_PER_KM * distance_km) + (FARE_PER_MINUTE * duration_min)

# Surge Pricing Strategy
class SurgePricing(PricingStrategy):
    def __init__(self, surge_multiplier):
        self.surge_multiplier = surge_multiplier

    def calculate_fare(self, distance_km, duration_min):
        base_fare = BASE_FARE + (FARE_PER_KM * distance_km) + (FARE_PER_MINUTE * duration_min)
        return base_fare * self.surge_multiplier

# Geolocation Class
class Geolocation:
    def __init__(self, lat, lon):
        self.lat = lat
        self.lon = lon

    def distance_to(self, other):
        lat1, lon1 = radians(self.lat), radians(self.lon)
        lat2, lon2 = radians(other.lat), radians(other.lon)

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        return EARTH_RADIUS_KM * c

# Ride Class
class Ride:
    def __init__(self, ride_id, rider, driver, pickup_location, dropoff_location):
        self.ride_id = ride_id
        self.rider = rider
        self.driver = driver
        self.pickup_location = pickup_location
        self.dropoff_location = dropoff_location
        self.status = "Requested"
        self.fare = None
        self.start_time = None
        self.end_time = None

    def start_ride(self):
        self.status = "Started"
        self.start_time = datetime.now()
        with print_lock:
            print(f"Ride {self.ride_id} started at {self.start_time}.")

    def end_ride(self):
        self.status = "Completed"
        self.end_time = datetime.now()
        with print_lock:
            print(f"Ride {self.ride_id} completed at {self.end_time}. Fare: ${self.fare:.2f}")

    def cancel_ride(self):
        if self.status == "Requested":
            self.status = "Cancelled"
            with print_lock:
                print(f"Ride {self.ride_id} has been cancelled.")
        else:
            with print_lock:
                print(f"Ride {self.ride_id} cannot be cancelled. Current status: {self.status}")

# Rider Class
class Rider:
    def __init__(self, rider_id, name, location):
        self.rider_id = rider_id
        self.name = name
        self.location = location

    def request_ride(self, dropoff_location):
        ride_id = random.randint(1000, 9999)
        with print_lock:
            print(f"Ride {ride_id} requested by {self.name}.")
        ride_request_queue.put((self, dropoff_location, ride_id))

# Driver Class
class Driver:
    def __init__(self, driver_id, name, location):
        self.driver_id = driver_id
        self.name = name
        self.location = location
        self.available = True

    def accept_ride(self, ride):
        if self.available:
            self.available = False
            ride.driver = self
            ride.start_ride()
            distance_km = self.location.distance_to(ride.dropoff_location)
            duration_min = random.randint(5, 30)  # Simulate ride duration
            fare = pricing_engine.calculate_fare(distance_km, duration_min)
            ride.fare = fare
            time.sleep(duration_min * 0.1)  # Simulate ride time
            ride.end_ride()
            self.available = True
        else:
            with print_lock:
                print(f"Driver {self.name} is not available.")

# Dispatch System
class DispatchSystem:
    def __init__(self):
        self.drivers = []

    def add_driver(self, driver):
        self.drivers.append(driver)

    def find_nearest_driver(self, location):
        nearest_driver = None
        min_distance = float('inf')

        for driver in self.drivers:
            if driver.available:
                distance = driver.location.distance_to(location)
                if distance < min_distance:
                    min_distance = distance
                    nearest_driver = driver

        return nearest_driver

    def process_ride_requests(self):
        while True:
            if not ride_request_queue.empty():
                rider, dropoff_location, ride_id = ride_request_queue.get()
                nearest_driver = self.find_nearest_driver(rider.location)
                if nearest_driver:
                    ride = Ride(ride_id, rider, nearest_driver, rider.location, dropoff_location)
                    nearest_driver.accept_ride(ride)
                else:
                    with print_lock:
                        print(f"No available drivers for Ride {ride_id}.")
            else:
                time.sleep(1)  # Wait for new ride requests

# Pricing Engine
class PricingEngine:
    def __init__(self, strategy: PricingStrategy):
        self.strategy = strategy

    def calculate_fare(self, distance_km, duration_min):
        return self.strategy.calculate_fare(distance_km, duration_min)

# Simulate ride requests
def simulate_ride_requests():
    riders = [
        Rider(1, "Alice", Geolocation(37.7749, -122.4194)),
        Rider(2, "Bob", Geolocation(34.0522, -118.2437)),
    ]

    for rider in riders:
        dropoff_location = Geolocation(random.uniform(34, 38), random.uniform(-123, -118))
        rider.request_ride(dropoff_location)
        time.sleep(1)

# Main Function
def main():
    # Initialize pricing engine with base pricing
    global pricing_engine
    pricing_engine = PricingEngine(BasePricing())

    # Initialize dispatch system
    dispatch_system = DispatchSystem()

    # Add drivers
    drivers = [
        Driver(1, "John", Geolocation(37.7749, -122.4194)),
        Driver(2, "Jane", Geolocation(34.0522, -118.2437)),
    ]
    for driver in drivers:
        dispatch_system.add_driver(driver)

    # Start dispatch system thread
    dispatch_thread = threading.Thread(target=dispatch_system.process_ride_requests, daemon=True)
    dispatch_thread.start()

    # Simulate ride requests
    simulate_ride_requests()

    # Keep the main thread alive to allow background processing
    time.sleep(20)

if __name__ == "__main__":
    main()