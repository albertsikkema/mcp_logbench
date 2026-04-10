from __future__ import annotations

from unittest.mock import patch

from mcp_logbench.config import RateLimitConfig
from mcp_logbench.rate_limit import RateLimiter


def test_acquire_within_burst() -> None:
    limiter = RateLimiter(RateLimitConfig(queries_per_minute=60, burst=3))
    assert limiter.acquire() is True
    assert limiter.acquire() is True
    assert limiter.acquire() is True


def test_acquire_exceeds_burst() -> None:
    limiter = RateLimiter(RateLimitConfig(queries_per_minute=60, burst=2))
    assert limiter.acquire() is True
    assert limiter.acquire() is True
    assert limiter.acquire() is False


def test_tokens_refill_over_time() -> None:
    limiter = RateLimiter(RateLimitConfig(queries_per_minute=60, burst=1))
    assert limiter.acquire() is True
    assert limiter.acquire() is False

    # Advance time by 1 second (1 token/sec at 60 qpm)
    with patch("mcp_logbench.rate_limit.time") as mock_time:
        mock_time.monotonic.return_value = limiter._last_refill + 1.0
        assert limiter.acquire() is True


def test_retry_after_returns_positive_when_empty() -> None:
    limiter = RateLimiter(RateLimitConfig(queries_per_minute=60, burst=1))
    limiter.acquire()  # drain
    retry = limiter.retry_after()
    assert retry > 0.0


def test_retry_after_returns_zero_when_available() -> None:
    limiter = RateLimiter(RateLimitConfig(queries_per_minute=60, burst=3))
    assert limiter.retry_after() == 0.0
