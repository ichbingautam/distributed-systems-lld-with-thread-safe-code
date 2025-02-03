import time
from abc import ABC, abstractmethod
from collections import deque
from threading import Lock

# Rate Limiter Interface
class RateLimiter(ABC):
    @abstractmethod
    def allow_request(self, user_id):
        pass

# Token Bucket Algorithm
class TokenBucketRateLimiter(RateLimiter):
    def __init__(self, max_tokens, refill_rate):
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate
        self.tokens = max_tokens
        self.last_refill_time = time.time()
        self.lock = Lock()

    def allow_request(self, user_id):
        with self.lock:
            self._refill_tokens()
            if self.tokens > 0:
                self.tokens -= 1
                return True
            return False

    def _refill_tokens(self):
        now = time.time()
        time_elapsed = now - self.last_refill_time
        tokens_to_add = time_elapsed * self.refill_rate
        self.tokens = min(self.tokens + tokens_to_add, self.max_tokens)
        self.last_refill_time = now

# Sliding Window Algorithm
class SlidingWindowRateLimiter(RateLimiter):
    def __init__(self, max_requests, window_size_seconds):
        self.max_requests = max_requests
        self.window_size_seconds = window_size_seconds
        self.request_logs = {}
        self.lock = Lock()

    def allow_request(self, user_id):
        with self.lock:
            current_time = time.time()
            if user_id not in self.request_logs:
                self.request_logs[user_id] = deque()

            # Remove outdated requests
            while self.request_logs[user_id] and self.request_logs[user_id][0] < current_time - self.window_size_seconds:
                self.request_logs[user_id].popleft()

            if len(self.request_logs[user_id]) < self.max_requests:
                self.request_logs[user_id].append(current_time)
                return True
            return False

# Distributed Rate Limiter (Using Redis as a shared store)
class DistributedRateLimiter(RateLimiter):
    def __init__(self, max_requests, window_size_seconds, redis_client):
        self.max_requests = max_requests
        self.window_size_seconds = window_size_seconds
        self.redis_client = redis_client

    def allow_request(self, user_id):
        current_time = time.time()
        key = f"rate_limit:{user_id}"
        with self.redis_client.pipeline() as pipe:
            # Remove outdated requests
            pipe.zremrangebyscore(key, 0, current_time - self.window_size_seconds)
            # Count remaining requests
            pipe.zcard(key)
            # Add current request
            pipe.zadd(key, {current_time: current_time})
            pipe.expire(key, self.window_size_seconds)
            results = pipe.execute()
            request_count = results[1]
            if request_count < self.max_requests:
                return True
            return False

# Simulate API Requests
def simulate_api_requests(rate_limiter, user_id, num_requests):
    for i in range(num_requests):
        if rate_limiter.allow_request(user_id):
            print(f"Request {i + 1} from User {user_id} allowed.")
        else:
            print(f"Request {i + 1} from User {user_id} denied.")
        time.sleep(0.1)

# Main Function
def main():
    # Token Bucket Example
    print("Token Bucket Rate Limiter:")
    token_bucket_limiter = TokenBucketRateLimiter(max_tokens=5, refill_rate=1)
    simulate_api_requests(token_bucket_limiter, user_id=1, num_requests=10)

    # Sliding Window Example
    print("\nSliding Window Rate Limiter:")
    sliding_window_limiter = SlidingWindowRateLimiter(max_requests=5, window_size_seconds=10)
    simulate_api_requests(sliding_window_limiter, user_id=2, num_requests=10)

    # Distributed Rate Limiter Example (Requires Redis)
    # Uncomment to test with a Redis server
    """
    import redis
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    print("\nDistributed Rate Limiter:")
    distributed_limiter = DistributedRateLimiter(max_requests=5, window_size_seconds=10, redis_client=redis_client)
    simulate_api_requests(distributed_limiter, user_id=3, num_requests=10)
    """

if __name__ == "__main__":
    main()