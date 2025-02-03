import time
import threading
from abc import ABC, abstractmethod
from queue import Queue
from typing import Dict, List, Optional
import random

# Constants
BASE_FARE_PER_KM = 2.0  # Base fare per kilometer in dollars

# Location Data
class Location:
    def __init__(self, latitude: float, longitude: float):
        self.latitude = latitude
        self.longitude = longitude

    def __str__(self):
        return f"({self.latitude}, {self.longitude})"

# Ride Request
class RideRequest:
    def __init__(self, user_id: str, pickup_location: Location, dropoff_location: Location):
        self.user_id = user_id
        self.pickup_location = pickup_location
        self.dropoff_location = dropoff_location
        self.timestamp = time.time()

# Driver
class Driver:
    def __init__(self, driver_id: str, location: Location):
        self.driver_id = driver_id
        self.location = location
        self.available = True

# Ride
class Ride:
    def __init__(self, ride_id: str, driver: Driver, riders: List[RideRequest]):
        self.ride_id = ride_id
        self.driver = driver
        self.riders = riders
        self.status = "Scheduled"
        self.start_time = None
        self.end_time = None
        self.fare = self.calculate_fare()

    def calculate_fare(self) -> Dict[str, float]:
        # Calculate fare based on distance traveled by each rider
        fares = {}
        total_distance = self._calculate_total_distance()
        base_fare = total_distance * BASE_FARE_PER_KM

        # Split fare based on distance traveled by each rider
        for rider in self.riders:
            rider_distance = self._calculate_distance(rider.pickup_location, rider.dropoff_location)
            fares[rider.user_id] = (rider_distance / total_distance) * base_fare

        return fares

    def _calculate_total_distance(self) -> float:
        # Simulate total distance calculation (in kilometers)
        return random.uniform(5, 20)  # Random distance between 5 and 20 km

    def _calculate_distance(self, loc1: Location, loc2: Location) -> float:
        # Simulate distance calculation between two locations (in kilometers)
        return random.uniform(1, 10)  # Random distance between 1 and 10 km

    def start_ride(self):
        self.status = "Started"
        self.start_time = time.time()
        print(f"Ride {self.ride_id} started at {self.start_time}.")

    def end_ride(self):
        self.status = "Completed"
        self.end_time = time.time()
        print(f"Ride {self.ride_id} completed at {self.end_time}.")
        for user_id, fare in self.fare.items():
            print(f"User {user_id} fare: ${fare:.2f}")

    def cancel_ride(self, user_id: str):
        if self.status == "Started":
            print(f"Ride {self.ride_id} cannot be cancelled mid-ride by User {user_id}.")
        else:
            self.riders = [rider for rider in self.riders if rider.user_id != user_id]
            if not self.riders:
                self.status = "Cancelled"
                print(f"Ride {self.ride_id} cancelled by User {user_id}.")
            else:
                self.fare = self.calculate_fare()  # Recalculate fare
                print(f"User {user_id} cancelled. Updated fares:")
                for rider_id, fare in self.fare.items():
                    print(f"User {rider_id} fare: ${fare:.2f}")

# Ride Matching Strategy Interface
class RideMatchingStrategy(ABC):
    @abstractmethod
    def match_riders(self, ride_requests: List[RideRequest], driver: Driver) -> Optional[List[RideRequest]]:
        pass

# Default Ride Matching Strategy
class DefaultRideMatchingStrategy(RideMatchingStrategy):
    def match_riders(self, ride_requests: List[RideRequest], driver: Driver) -> Optional[List[RideRequest]]:
        # Simulate matching riders traveling in the same direction
        if len(ride_requests) >= 2:
            return ride_requests[:2]  # Match first two riders
        return None

# Ride Service
class RideService:
    def __init__(self):
        self.ride_requests: List[RideRequest] = []
        self.drivers: List[Driver] = []
        self.rides: Dict[str, Ride] = {}
        self.ride_matching_strategy = DefaultRideMatchingStrategy()
        self.lock = threading.Lock()

    def add_ride_request(self, user_id: str, pickup_location: Location, dropoff_location: Location):
        with self.lock:
            ride_request = RideRequest(user_id, pickup_location, dropoff_location)
            self.ride_requests.append(ride_request)
            print(f"Ride request added by User {user_id} from {pickup_location} to {dropoff_location}.")

    def add_driver(self, driver_id: str, location: Location):
        with self.lock:
            driver = Driver(driver_id, location)
            self.drivers.append(driver)
            print(f"Driver {driver_id} added at {location}.")

    def match_rides(self):
        with self.lock:
            for driver in self.drivers:
                if driver.available:
                    matched_riders = self.ride_matching_strategy.match_riders(self.ride_requests, driver)
                    if matched_riders:
                        ride_id = f"ride_{len(self.rides) + 1}"
                        ride = Ride(ride_id, driver, matched_riders)
                        self.rides[ride_id] = ride
                        driver.available = False
                        self.ride_requests = [r for r in self.ride_requests if r not in matched_riders]
                        print(f"Ride {ride_id} created with Driver {driver.driver_id} and Riders: {[r.user_id for r in matched_riders]}")

    def start_ride(self, ride_id: str):
        with self.lock:
            if ride_id in self.rides:
                self.rides[ride_id].start_ride()

    def end_ride(self, ride_id: str):
        with self.lock:
            if ride_id in self.rides:
                self.rides[ride_id].end_ride()
                self.drivers = [d for d in self.drivers if d.driver_id != self.rides[ride_id].driver.driver_id]

    def cancel_ride(self, ride_id: str, user_id: str):
        with self.lock:
            if ride_id in self.rides:
                self.rides[ride_id].cancel_ride(user_id)

# Simulate Ride Requests and Drivers
def simulate_ride_requests(ride_service: RideService):
    locations = [
        Location(37.7749, -122.4194),  # San Francisco
        Location(34.0522, -118.2437),  # Los Angeles
        Location(36.1699, -115.1398),  # Las Vegas
    ]
    for i in range(10):  # Simulate 10 ride requests
        user_id = f"user_{i}"
        pickup_location = random.choice(locations)
        dropoff_location = random.choice(locations)
        ride_service.add_ride_request(user_id, pickup_location, dropoff_location)
        time.sleep(random.uniform(0.1, 0.5))  # Simulate random request intervals

def simulate_drivers(ride_service: RideService):
    locations = [
        Location(37.7749, -122.4194),  # San Francisco
        Location(34.0522, -118.2437),  # Los Angeles
        Location(36.1699, -115.1398),  # Las Vegas
    ]
    for i in range(3):  # Simulate 3 drivers
        driver_id = f"driver_{i}"
        location = random.choice(locations)
        ride_service.add_driver(driver_id, location)
        time.sleep(random.uniform(1, 2))  # Simulate random driver availability

# Main Function
def main():
    ride_service = RideService()

    # Simulate ride requests and drivers in separate threads
    threading.Thread(target=simulate_ride_requests, args=(ride_service,), daemon=True).start()
    threading.Thread(target=simulate_drivers, args=(ride_service,), daemon=True).start()

    # Continuously match rides
    while True:
        ride_service.match_rides()
        time.sleep(2)  # Match rides every 2 seconds

if __name__ == "__main__":
    main()