import time
import threading
from abc import ABC, abstractmethod
from typing import Dict, Optional, List
import hashlib

# Cache Node
class CacheNode:
    def __init__(self, node_id: str, eviction_policy: str = "LRU", max_size: int = 100):
        self.node_id = node_id
        self.max_size = max_size
        self.cache: Dict[str, str] = {}
        self.eviction_policy = eviction_policy
        self.access_times: Dict[str, float] = {}  # For LRU
        self.access_counts: Dict[str, int] = {}  # For LFU
        self.ttl: Dict[str, float] = {}  # For TTL

    def get(self, key: str) -> Optional[str]:
        if key in self.cache:
            # Check TTL (if applicable)
            if self.eviction_policy == "TTL" and key in self.ttl:
                if time.time() > self.ttl[key]:
                    self.invalidate(key)
                    return None

            # Update access time/count for eviction policies
            if self.eviction_policy == "LRU":
                self.access_times[key] = time.time()
            elif self.eviction_policy == "LFU":
                self.access_counts[key] += 1
            return self.cache[key]
        return None

    def set(self, key: str, value: str, ttl: Optional[float] = None):
        if len(self.cache) >= self.max_size:
            self._evict()
        self.cache[key] = value
        if self.eviction_policy == "LRU":
            self.access_times[key] = time.time()
        elif self.eviction_policy == "LFU":
            self.access_counts[key] = 1
        elif self.eviction_policy == "TTL" and ttl is not None:
            self.ttl[key] = time.time() + ttl

    def _evict(self):
        if self.eviction_policy == "LRU":
            # Evict the least recently used item
            key_to_evict = min(self.access_times, key=self.access_times.get)
            self.invalidate(key_to_evict)
        elif self.eviction_policy == "LFU":
            # Evict the least frequently used item
            key_to_evict = min(self.access_counts, key=self.access_counts.get)
            self.invalidate(key_to_evict)
        elif self.eviction_policy == "TTL":
            # Evict expired items
            current_time = time.time()
            expired_keys = [key for key, expiry in self.ttl.items() if expiry <= current_time]
            for key in expired_keys:
                self.invalidate(key)

    def invalidate(self, key: str):
        if key in self.cache:
            del self.cache[key]
            if key in self.access_times:
                del self.access_times[key]
            if key in self.access_counts:
                del self.access_counts[key]
            if key in self.ttl:
                del self.ttl[key]

# Distributed Cache
class DistributedCache:
    def __init__(self, nodes: List[CacheNode]):
        self.nodes = nodes
        self.node_map = self._create_node_map()

    def _create_node_map(self) -> Dict[int, CacheNode]:
        # Use consistent hashing to map keys to nodes
        node_map = {}
        for node in self.nodes:
            for i in range(3):  # Add 3 virtual nodes per physical node
                hash_key = self._hash(f"{node.node_id}_{i}")
                node_map[hash_key] = node
        return node_map

    def _hash(self, key: str) -> int:
        # Use SHA-256 for consistent hashing
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)

    def _get_node(self, key: str) -> CacheNode:
        # Find the node responsible for the key
        hash_key = self._hash(key)
        sorted_keys = sorted(self.node_map.keys())
        for node_key in sorted_keys:
            if hash_key <= node_key:
                return self.node_map[node_key]
        return self.node_map[sorted_keys[0]]  # Wrap around to the first node

    def get(self, key: str) -> Optional[str]:
        node = self._get_node(key)
        return node.get(key)

    def set(self, key: str, value: str, ttl: Optional[float] = None):
        node = self._get_node(key)
        node.set(key, value, ttl)

    def invalidate(self, key: str):
        node = self._get_node(key)
        node.invalidate(key)

# Pub/Sub System for Cache Invalidation
class PubSubSystem:
    def __init__(self):
        self.subscribers: Dict[str, List[CacheNode]] = {}

    def subscribe(self, channel: str, node: CacheNode):
        if channel not in self.subscribers:
            self.subscribers[channel] = []
        self.subscribers[channel].append(node)

    def publish(self, channel: str, message: str):
        if channel in self.subscribers:
            for node in self.subscribers[channel]:
                node.invalidate(message)

# Simulate Cache Usage
def simulate_cache_usage(cache: DistributedCache, pubsub: PubSubSystem):
    # Simulate setting and getting cache values
    cache.set("driver_1", "available", ttl=5)  # TTL of 5 seconds
    cache.set("user_1", "profile_data")
    cache.set("route_1", "route_details")

    print(f"Driver 1 status: {cache.get('driver_1')}")
    print(f"User 1 profile: {cache.get('user_1')}")
    print(f"Route 1 details: {cache.get('route_1')}")

    # Simulate cache invalidation via pub/sub
    pubsub.publish("cache_invalidation", "driver_1")
    print(f"Driver 1 status after invalidation: {cache.get('driver_1')}")

    # Simulate TTL expiration
    time.sleep(6)  # Wait for TTL to expire
    print(f"Driver 1 status after TTL expiration: {cache.get('driver_1')}")

# Main Function
def main():
    # Create cache nodes
    node1 = CacheNode("node1", eviction_policy="TTL", max_size=10)
    node2 = CacheNode("node2", eviction_policy="LRU", max_size=10)
    node3 = CacheNode("node3", eviction_policy="LFU", max_size=10)

    # Create distributed cache
    cache = DistributedCache([node1, node2, node3])

    # Create pub/sub system for cache invalidation
    pubsub = PubSubSystem()
    pubsub.subscribe("cache_invalidation", node1)
    pubsub.subscribe("cache_invalidation", node2)
    pubsub.subscribe("cache_invalidation", node3)

    # Simulate cache usage
    simulate_cache_usage(cache, pubsub)

if __name__ == "__main__":
    main()