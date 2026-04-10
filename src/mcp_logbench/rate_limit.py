from __future__ import annotations

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mcp_logbench.config import RateLimitConfig


class RateLimiter:
    """In-process token bucket rate limiter."""

    def __init__(self, config: RateLimitConfig) -> None:
        self._rate = config.queries_per_minute / 60.0  # tokens per second
        self._capacity = config.burst
        self._tokens = float(config.burst)
        self._last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now

    def acquire(self) -> bool:
        self._refill()
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    def retry_after(self) -> float:
        self._refill()
        if self._tokens >= 1.0:
            return 0.0
        deficit = 1.0 - self._tokens
        return deficit / self._rate if self._rate > 0 else float("inf")
