import time
import threading
from abc import ABC, abstractmethod
from queue import Queue
from typing import Dict, List
import random

# Constants
BASE_FARE = 5.0  # Base fare in dollars
MAX_SURGE_MULTIPLIER = 3.0  # Maximum surge multiplier to avoid user dissatisfaction
SURGE_THRESHOLDS = {
    "low": 1.0,  # No surge
    "medium": 1.5,  # Moderate surge
    "high": 2.0,  # High surge
}
SURGE_EXPLANATIONS = {
    "low": "No surge pricing applied.",
    "medium": "Moderate surge due to increased demand.",
    "high": "High surge due to very high demand.",
}

# Ride Request
class RideRequest:
    def __init__(self, user_id: str, location: str):
        self.user_id = user_id
        self.location = location
        self.timestamp = time.time()

# Driver
class Driver:
    def __init__(self, driver_id: str, location: str):
        self.driver_id = driver_id
        self.location = location
        self.available = True

# Surge Pricing Strategy Interface
class SurgePricingStrategy(ABC):
    @abstractmethod
    def calculate_surge_multiplier(self, demand: int, supply: int) -> float:
        pass

    @abstractmethod
    def get_surge_explanation(self, demand: int, supply: int) -> str:
        pass

# Default Surge Pricing Strategy
class DefaultSurgePricingStrategy(SurgePricingStrategy):
    def calculate_surge_multiplier(self, demand: int, supply: int) -> float:
        if supply == 0:
            return MAX_SURGE_MULTIPLIER  # No drivers available
        demand_supply_ratio = demand / supply
        if demand_supply_ratio < 1.0:
            return SURGE_THRESHOLDS["low"]
        elif demand_supply_ratio < 2.0:
            return SURGE_THRESHOLDS["medium"]
        else:
            return min(SURGE_THRESHOLDS["high"], MAX_SURGE_MULTIPLIER)

    def get_surge_explanation(self, demand: int, supply: int) -> str:
        if supply == 0:
            return "No drivers available. Surge pricing at maximum."
        demand_supply_ratio = demand / supply
        if demand_supply_ratio < 1.0:
            return SURGE_EXPLANATIONS["low"]
        elif demand_supply_ratio < 2.0:
            return SURGE_EXPLANATIONS["medium"]
        else:
            return SURGE_EXPLANATIONS["high"]

# Pricing Engine
class PricingEngine:
    def __init__(self, surge_pricing_strategy: SurgePricingStrategy):
        self.surge_pricing_strategy = surge_pricing_strategy

    def calculate_fare(self, base_fare: float, demand: int, supply: int) -> float:
        surge_multiplier = self.surge_pricing_strategy.calculate_surge_multiplier(demand, supply)
        return base_fare * surge_multiplier

    def get_surge_explanation(self, demand: int, supply: int) -> str:
        return self.surge_pricing_strategy.get_surge_explanation(demand, supply)

# Ride Service
class RideService:
    def __init__(self):
        self.ride_requests: Dict[str, List[RideRequest]] = {}  # Location -> List of ride requests
        self.drivers: Dict[str, List[Driver]] = {}  # Location -> List of drivers
        self.pricing_engine = PricingEngine(DefaultSurgePricingStrategy())
        self.lock = threading.Lock()

    def add_ride_request(self, user_id: str, location: str):
        with self.lock:
            if location not in self.ride_requests:
                self.ride_requests[location] = []
            self.ride_requests[location].append(RideRequest(user_id, location))
            print(f"Ride request added by User {user_id} in {location}.")

    def add_driver(self, driver_id: str, location: str):
        with self.lock:
            if location not in self.drivers:
                self.drivers[location] = []
            self.drivers[location].append(Driver(driver_id, location))
            print(f"Driver {driver_id} added in {location}.")

    def calculate_fare(self, location: str) -> float:
        with self.lock:
            demand = len(self.ride_requests.get(location, []))
            supply = len(self.drivers.get(location, []))
            fare = self.pricing_engine.calculate_fare(BASE_FARE, demand, supply)
            explanation = self.pricing_engine.get_surge_explanation(demand, supply)
            print(f"Calculated fare for {location}: ${fare:.2f} (Demand: {demand}, Supply: {supply})")
            print(f"Explanation: {explanation}")
            return fare

# Simulate Ride Requests and Drivers
def simulate_ride_requests(ride_service: RideService):
    locations = ["Downtown", "Airport", "Suburb"]
    for i in range(20):  # Simulate 20 ride requests
        user_id = f"user_{i}"
        location = random.choice(locations)
        ride_service.add_ride_request(user_id, location)
        time.sleep(random.uniform(0.1, 0.5))  # Simulate random request intervals

def simulate_drivers(ride_service: RideService):
    locations = ["Downtown", "Airport", "Suburb"]
    for i in range(5):  # Simulate 5 drivers
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

    # Continuously calculate fares for each location
    locations = ["Downtown", "Airport", "Suburb"]
    while True:
        for location in locations:
            ride_service.calculate_fare(location)
        time.sleep(5)  # Recalculate fares every 5 seconds

if __name__ == "__main__":
    main()