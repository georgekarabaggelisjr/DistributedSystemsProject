"""
Database connection lifecycle and dependency injection module.

This module manages the lifecycle of the asynchronous PostgreSQL connection
pool using the `asyncpg` library. It implements a singleton pattern for the
connection pool to ensure efficient resource utilization across the FastAPI
application.

The database pool must be initialized during the application startup lifespan
and closed during shutdown to prevent leaked connections.
"""

import logging
from typing import Optional

import asyncpg
from app.core.config import settings

# Initialize module-level logger for database events
logger = logging.getLogger(__name__)

# Private module-level variable to hold the singleton pool instance
_global_db_pool: Optional[asyncpg.Pool] = None


async def setup_database() -> None:
    """
    Initializes the global asynchronous database connection pool.

    Creates a connection pool based on the `DATABASE_URL` provided in the
    application settings. This pool is shared across all request handlers
    to maintain high performance and low latency.

    :raises Exception: If the connection to the PostgreSQL server cannot be
        established or the pool configuration is invalid.
    """
    global _global_db_pool

    logger.info("Initializing PostgreSQL connection pool...")
    try:
        # Standard pool sizes for local and containerized environments.
        # We await create_pool because it returns a Pool object which is awaitable.
        _global_db_pool = await asyncpg.create_pool(  # type: ignore[misc]
            dsn=settings.DATABASE_URL,
            min_size=1,
            max_size=10
        )
        logger.info("PostgreSQL connection pool established successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {str(e)}")
        raise


async def teardown_database() -> None:
    """
    Gracefully closes all active connections in the global pool.

    This function should be registered as a shutdown event. It ensures that
    all existing database transactions are concluded and sockets are
    cleanly released before the process terminates.
    """
    global _global_db_pool
    if _global_db_pool:
        logger.info("Closing PostgreSQL connection pool...")
        await _global_db_pool.close()
        _global_db_pool = None
        logger.info("PostgreSQL connection pool closed.")


async def get_db_pool() -> asyncpg.Pool:
    """
    FastAPI Dependency that provides the active database pool.

    Use this as a dependency in FastAPI route handlers to gain access to
    the PostgreSQL state store.

    :return: The initialized and active singleton database pool.
    :rtype: asyncpg.Pool
    :raises RuntimeError: If accessed before the pool has been initialized
        via `setup_database`.
    """
    if _global_db_pool is None:
        logger.error("Attempted to access database pool before initialization.")
        raise RuntimeError(
            "Database pool is not initialized. Ensure 'setup_database' "
            "is called during the application lifespan."
        )

    return _global_db_pool