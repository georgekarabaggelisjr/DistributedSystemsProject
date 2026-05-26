"""
Distributed cache lifecycle and dependency injection module.

This module manages the asynchronous connection pool for Redis using
the `redis.asyncio` library. It implements a robust singleton pattern
to provide a persistent, high-performance in-memory datastore for
atomic counting and phase barrier synchronization.
"""

import logging
from typing import Optional

import redis.asyncio as redis
from app.core.config import settings

# Initialize module-level logger for cache events
logger = logging.getLogger(__name__)

# Private module-level variable to hold the singleton pool instance
_global_redis_client: Optional[redis.Redis] = None


async def setup_redis() -> None:
    """
    Initializes the global asynchronous Redis connection pool.

    Establishes a robust connection pool to the Redis broker specified
    in the application settings. This pool prevents socket exhaustion
    during high-throughput worker acknowledgment bursts.

    :return: None
    :rtype: None
    :raises Exception: If the connection to the Redis server cannot be
        established or authenticated.
    """
    global _global_redis_client
    logger.info("Initializing Redis connection pool...")
    try:
        # decode_responses=True ensures strings are returned instead of bytes.
        client = redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_timeout=5.0
        )

        # Ping the server to proactively verify connectivity.
        # Awaitable/Bool union type returned by the redis-py ping method.
        await client.ping()  # type: ignore[func-returns-value, attr-defined]

        _global_redis_client = client
        logger.info("Redis connection pool established successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize Redis pool: {str(e)}")
        raise


async def teardown_redis() -> None:
    """
    Gracefully closes the active Redis connection pool.

    Ensures that all pending commands are flushed and sockets are
    cleanly released before the application terminates.

    :return: None
    :rtype: None
    """
    global _global_redis_client
    if _global_redis_client:
        logger.info("Closing Redis connection pool...")
        await _global_redis_client.aclose()
        _global_redis_client = None
        logger.info("Redis connection pool closed.")


async def get_redis_client() -> redis.Redis:
    """
    FastAPI Dependency that provides the active Redis client.

    Use this as a dependency in route handlers or background services
    to interact with the distributed cache.

    :return: The initialized and active singleton Redis client.
    :rtype: redis.Redis
    :raises RuntimeError: If accessed before the client has been initialized
        via `setup_redis`.
    """
    if _global_redis_client is None:
        logger.error("Attempted to access Redis client before initialization.")
        raise RuntimeError(
            "Redis client is not initialized. Ensure 'setup_redis' "
            "is called during the application lifespan."
        )

    return _global_redis_client