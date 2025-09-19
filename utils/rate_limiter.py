import time
import threading
from typing import Optional

class RateLimiter:
    """Rate limiter to respect API rate limits"""
    
    def __init__(self, requests_per_second: float = 1.0):
        """
        Initialize rate limiter
        
        Args:
            requests_per_second: Maximum number of requests per second
        """
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0
        self.last_request_time = 0.0
        self._lock = threading.Lock()
    
    def wait(self):
        """Wait if necessary to respect rate limit"""
        with self._lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            
            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last
                time.sleep(sleep_time)
                self.last_request_time = time.time()
            else:
                self.last_request_time = current_time
    
    def update_rate(self, requests_per_second: float):
        """Update the rate limit"""
        with self._lock:
            self.requests_per_second = requests_per_second
            self.min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0

class BurstRateLimiter:
    """Rate limiter that allows bursts up to a certain limit"""
    
    def __init__(self, requests_per_second: float = 2.0, burst_size: int = 10):
        """
        Initialize burst rate limiter
        
        Args:
            requests_per_second: Sustained requests per second
            burst_size: Maximum number of requests in a burst
        """
        self.requests_per_second = requests_per_second
        self.burst_size = burst_size
        self.tokens = burst_size
        self.last_update = time.time()
        self._lock = threading.Lock()
    
    def wait(self):
        """Wait if necessary to respect rate limit"""
        with self._lock:
            current_time = time.time()
            
            # Add tokens based on elapsed time
            elapsed = current_time - self.last_update
            self.tokens = min(
                self.burst_size,
                self.tokens + elapsed * self.requests_per_second
            )
            self.last_update = current_time
            
            # If we have tokens, use one
            if self.tokens >= 1:
                self.tokens -= 1
                return
            
            # Otherwise, wait until we can get a token
            wait_time = (1 - self.tokens) / self.requests_per_second
            time.sleep(wait_time)
            self.tokens = 0
            self.last_update = time.time()

class AdaptiveRateLimiter:
    """Rate limiter that adapts based on API responses"""
    
    def __init__(self, initial_requests_per_second: float = 2.0):
        """
        Initialize adaptive rate limiter
        
        Args:
            initial_requests_per_second: Initial rate limit
        """
        self.current_rate = initial_requests_per_second
        self.min_rate = 0.1  # Minimum 1 request per 10 seconds
        self.max_rate = 10.0  # Maximum 10 requests per second
        self.base_limiter = RateLimiter(initial_requests_per_second)
        self._lock = threading.Lock()
    
    def wait(self):
        """Wait if necessary to respect current rate limit"""
        self.base_limiter.wait()
    
    def on_success(self):
        """Called when a request succeeds - gradually increase rate"""
        with self._lock:
            # Gradually increase rate by 10%
            new_rate = min(self.max_rate, self.current_rate * 1.1)
            if new_rate != self.current_rate:
                self.current_rate = new_rate
                self.base_limiter.update_rate(new_rate)
    
    def on_rate_limit_error(self, retry_after: Optional[int] = None):
        """Called when hitting rate limit - decrease rate"""
        with self._lock:
            if retry_after:
                # Use server-provided retry time
                new_rate = 1.0 / retry_after
            else:
                # Halve the current rate
                new_rate = max(self.min_rate, self.current_rate * 0.5)
            
            self.current_rate = new_rate
            self.base_limiter.update_rate(new_rate)
    
    def on_error(self):
        """Called when a request fails - slightly decrease rate"""
        with self._lock:
            # Slightly decrease rate by 20%
            new_rate = max(self.min_rate, self.current_rate * 0.8)
            if new_rate != self.current_rate:
                self.current_rate = new_rate
                self.base_limiter.update_rate(new_rate)
    
    def get_current_rate(self) -> float:
        """Get current rate limit"""
        return self.current_rate
