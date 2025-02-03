import time
import threading
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import random
from queue import Queue

# Driver Metrics
class DriverMetrics:
    def __init__(self, driver_id: str):
        self.driver_id = driver_id
        self.rating = 5.0  # Initial rating
        self.rides_completed = 0
        self.earnings = 0.0

    def update_rating(self, new_rating: float):
        self.rating = (self.rating * self.rides_completed + new_rating) / (self.rides_completed + 1)

    def complete_ride(self, earnings: float):
        self.rides_completed += 1
        self.earnings += earnings

    def calculate_score(self) -> float:
        # Weighted score: 50% rating, 30% rides completed, 20% earnings
        return (self.rating * 0.5) + (self.rides_completed * 0.3) + (self.earnings * 0.2)

# Leaderboard Node (Shard)
class LeaderboardNode:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.driver_metrics: Dict[str, DriverMetrics] = {}
        self.rankings: List[str] = []  # Sorted list of driver IDs
        self.lock = threading.Lock()

    def add_driver(self, driver_id: str):
        with self.lock:
            if driver_id not in self.driver_metrics:
                self.driver_metrics[driver_id] = DriverMetrics(driver_id)
                self._update_rankings()

    def update_rating(self, driver_id: str, new_rating: float):
        with self.lock:
            if driver_id in self.driver_metrics:
                self.driver_metrics[driver_id].update_rating(new_rating)
                self._update_rankings()

    def complete_ride(self, driver_id: str, earnings: float):
        with self.lock:
            if driver_id in self.driver_metrics:
                self.driver_metrics[driver_id].complete_ride(earnings)
                self._update_rankings()

    def _update_rankings(self):
        # Sort drivers by their weighted score in descending order
        self.rankings = sorted(
            self.driver_metrics.keys(),
            key=lambda x: self.driver_metrics[x].calculate_score(),
            reverse=True,
        )

    def get_top_drivers(self, n: int) -> List[str]:
        with self.lock:
            return self.rankings[:n]

# Distributed Leaderboard
class DistributedLeaderboard:
    def __init__(self, nodes: List[LeaderboardNode]):
        self.nodes = nodes
        self.node_map = self._create_node_map()

    def _create_node_map(self) -> Dict[int, LeaderboardNode]:
        # Use consistent hashing to map drivers to nodes
        node_map = {}
        for node in self.nodes:
            for i in range(3):  # Add 3 virtual nodes per physical node
                hash_key = self._hash(f"{node.node_id}_{i}")
                node_map[hash_key] = node
        return node_map

    def _hash(self, key: str) -> int:
        # Use SHA-256 for consistent hashing
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)

    def _get_node(self, driver_id: str) -> LeaderboardNode:
        # Find the node responsible for the driver
        hash_key = self._hash(driver_id)
        sorted_keys = sorted(self.node_map.keys())
        for node_key in sorted_keys:
            if hash_key <= node_key:
                return self.node_map[node_key]
        return self.node_map[sorted_keys[0]]  # Wrap around to the first node

    def add_driver(self, driver_id: str):
        node = self._get_node(driver_id)
        node.add_driver(driver_id)

    def update_rating(self, driver_id: str, new_rating: float):
        node = self._get_node(driver_id)
        node.update_rating(driver_id, new_rating)

    def complete_ride(self, driver_id: str, earnings: float):
        node = self._get_node(driver_id)
        node.complete_ride(driver_id, earnings)

    def get_top_drivers(self, n: int) -> List[str]:
        # Aggregate top drivers from all nodes
        all_drivers = []
        for node in self.nodes:
            all_drivers.extend(node.get_top_drivers(n))
        # Sort and return top N drivers
        return sorted(all_drivers, key=lambda x: self._get_node(x).driver_metrics[x].calculate_score(), reverse=True)[:n]

# Pub/Sub System for Real-Time Updates
class PubSubSystem:
    def __init__(self):
        self.subscribers: Dict[str, List[DistributedLeaderboard]] = {}

    def subscribe(self, channel: str, leaderboard: DistributedLeaderboard):
        if channel not in self.subscribers:
            self.subscribers[channel] = []
        self.subscribers[channel].append(leaderboard)

    def publish(self, channel: str, message: Dict):
        if channel in self.subscribers:
            for leaderboard in self.subscribers[channel]:
                if message["type"] == "add_driver":
                    leaderboard.add_driver(message["driver_id"])
                elif message["type"] == "update_rating":
                    leaderboard.update_rating(message["driver_id"], message["new_rating"])
                elif message["type"] == "complete_ride":
                    leaderboard.complete_ride(message["driver_id"], message["earnings"])

# Simulate Driver Activity
def simulate_driver_activity(pubsub: PubSubSystem):
    driver_ids = [f"driver_{i}" for i in range(10)]
    for driver_id in driver_ids:
        pubsub.publish("driver_updates", {"type": "add_driver", "driver_id": driver_id})

    while True:
        driver_id = random.choice(driver_ids)
        action = random.choice(["update_rating", "complete_ride"])
        if action == "update_rating":
            new_rating = random.uniform(4.0, 5.0)
            pubsub.publish("driver_updates", {"type": "update_rating", "driver_id": driver_id, "new_rating": new_rating})
        elif action == "complete_ride":
            earnings = random.uniform(10.0, 50.0)
            pubsub.publish("driver_updates", {"type": "complete_ride", "driver_id": driver_id, "earnings": earnings})
        time.sleep(1)  # Simulate activity every second

# Main Function
def main():
    # Create leaderboard nodes (shards)
    nodes = [LeaderboardNode(f"node_{i}") for i in range(3)]  # 3 shards
    distributed_leaderboard = DistributedLeaderboard(nodes)

    # Create pub/sub system for real-time updates
    pubsub = PubSubSystem()
    pubsub.subscribe("driver_updates", distributed_leaderboard)

    # Simulate driver activity in a separate thread
    threading.Thread(target=simulate_driver_activity, args=(pubsub,), daemon=True).start()

    # Continuously display the top 5 drivers
    while True:
        top_drivers = distributed_leaderboard.get_top_drivers(5)
        print("Top 5 Drivers:")
        for i, driver_id in enumerate(top_drivers, 1):
            print(f"{i}. {driver_id}")
        time.sleep(5)  # Refresh every 5 seconds

if __name__ == "__main__":
    main()