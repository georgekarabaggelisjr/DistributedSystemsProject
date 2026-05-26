"""
Unit tests for the distributed Cache Repository.

This suite utilizes asynchronous mocking to verify the atomic, Lua-based
tracking operations and ephemeral caching logic without requiring a physical
Redis connection. It validates the integrity of the Distributed Locks used
for the Phase Barrier mechanism, ensuring that progress tracking remains
consistent and immune to race conditions.

The module also includes verification for the P2P Shuffle Directory Service,
validating the aggregation and retrieval of ephemeral worker network endpoints.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from app.repositories.cache_repository import CacheRepository


@pytest.fixture
def mock_redis():
    """
    Provides a fully mocked asynchronous Redis client.

    This fixture simulates the redis-py-async interface, allowing for
    high-speed verification of atomic Lua script execution (EVAL), Hash
    operations (HSET/HVALS), lookups (HGET), and pipelines without network overhead.
    """
    redis_mock = AsyncMock()

    # Configure the pipeline mock
    pipe_mock = AsyncMock()
    # Mock the asynchronous context manager methods (__aenter__ and __aexit__)
    pipe_mock.__aenter__.return_value = pipe_mock
    pipe_mock.__aexit__.return_value = None

    # pipeline() is a sync method that returns the async context manager
    redis_mock.pipeline = MagicMock(return_value=pipe_mock)

    return redis_mock


@pytest.mark.asyncio
async def test_register_map_completion_atomic(mock_redis):
    """
    Verifies the atomic Lua-based tracking for the Map phase barrier.

    Ensures that the repository correctly executes the Lua script and accurately
    translates the integer response from Redis (1 or 0) into a boolean
    indicating whether the distributed lock was acquired.
    """
    # Simulate the Lua script returning 1 (Barrier Breached)
    mock_redis.eval.return_value = 1
    job_id = "test-job-123"
    task_id = "map-chunk-0"
    target_total = 5

    result = await CacheRepository.register_map_completion_atomic(
        job_id, task_id, target_total, mock_redis
    )

    # Assert that the DAL correctly translated the Redis response to a True boolean
    assert result is True
    # Verify that the EVAL command was dispatched with the correct script arguments
    mock_redis.eval.assert_called_once()
    args = mock_redis.eval.call_args[0]
    assert args[1] == 1  # Number of keys
    assert args[2] == f"job:{job_id}:map_completed_set"  # Key
    assert args[3] == task_id  # Arg 1
    assert args[4] == target_total  # Arg 2


@pytest.mark.asyncio
async def test_add_worker_endpoint(mock_redis):
    """
    Verifies the registration of worker network endpoints for P2P shuffle.

    Ensures that the repository correctly persists the network location of
    a worker mapped to its task_id in a dedicated Redis Hash and applies
    a 24-hour expiration for resource management.
    """
    job_id = "test-job-123"
    task_id = "map-chunk-0"
    endpoint = "10.0.1.45:8080"

    await CacheRepository.add_worker_endpoint(job_id, task_id, endpoint, mock_redis)

    # Extract the pipeline mock that was generated during the test execution
    pipe = mock_redis.pipeline.return_value

    # Verify atomic hash addition and expiration for the routing table on the pipeline object
    pipe.hset.assert_called_once_with(f"job:{job_id}:worker_endpoints", task_id, endpoint)
    pipe.expire.assert_called_once_with(f"job:{job_id}:worker_endpoints", 86400)
    pipe.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_worker_endpoints(mock_redis):
    """
    Verifies the retrieval and decoding of the P2P shuffle routing table.

    Validates that binary data returned by Redis is correctly decoded into
    UTF-8 strings and that empty maps are handled gracefully.
    """
    job_id = "test-job-123"
    # Redis-py returns bytes; DAL must ensure string conversion for the DTO
    mock_redis.hvals.return_value = [b"10.0.1.45:8080", b"10.0.1.46:8080"]

    result = await CacheRepository.get_worker_endpoints(job_id, mock_redis)

    assert result == ["10.0.1.45:8080", "10.0.1.46:8080"]
    mock_redis.hvals.assert_called_once_with(f"job:{job_id}:worker_endpoints")


@pytest.mark.asyncio
async def test_register_reduce_completion_atomic(mock_redis):
    """
    Verifies the atomic Lua-based tracking for the Reduce phase barrier.

    Validates that the repository correctly routes progress signals for the
    final aggregation phase and parses the boolean lock acquisition.
    """
    # Simulate the Lua script returning 0 (Barrier NOT Breached yet)
    mock_redis.eval.return_value = 0
    job_id = "test-job-123"
    task_id = "reduce-partition-1"
    target_total = 3

    result = await CacheRepository.register_reduce_completion_atomic(
        job_id, task_id, target_total, mock_redis
    )

    assert result is False
    mock_redis.eval.assert_called_once()


@pytest.mark.asyncio
async def test_get_cached_total_chunks_from_meta_hash(mock_redis):
    """
    Verifies the retrieval of cached barrier boundaries from the metadata hash.

    Ensures that the orchestrator pulls the total partition count from the
    correct 'progress_meta' key to evaluate phase completion.
    """
    mock_redis.hget.return_value = "10"
    job_id = "test-job-123"

    result = await CacheRepository.get_cached_total_chunks(job_id, mock_redis)

    # Assert type conversion: Redis returns strings; DAL must return integers
    assert result == 10
    mock_redis.hget.assert_called_once_with(f"job:{job_id}:progress_meta", "total_chunks")


@pytest.mark.asyncio
async def test_get_cached_total_chunks_missing(mock_redis):
    """
    Verifies the safe handling of cache misses for metadata lookups.

    Ensures that if the cache has expired or is not yet initialized,
    the repository returns None, triggering a fallback to the persistent DB layer.
    """
    mock_redis.hget.return_value = None
    job_id = "test-job-123"

    result = await CacheRepository.get_cached_total_chunks(job_id, mock_redis)

    assert result is None