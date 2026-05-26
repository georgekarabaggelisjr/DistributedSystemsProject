"""
Unit tests for the JobRepository data access layer.

This suite utilizes asynchronous mocking to verify the repository's logic,
state transition guardrails, and domain model instantiation without
requiring a physical database connection. It ensures that the
repository correctly handles the persistence of Job objects within the
multi-tenant architecture.
"""

import uuid
from typing import Any, Tuple

import asyncpg
import pytest
from unittest.mock import AsyncMock

from app.models.domain import Job
from app.repositories.job_repository import JobRepository


# --- FIXTURES ---

@pytest.fixture
def mock_db_pool(mocker: Any) -> Tuple[AsyncMock, AsyncMock]:
    """
    Creates a mocked asyncpg connection pool and connection.

    This fixture simulates the complex 'async with pool.acquire() as conn'
    and 'async with connection.transaction()' patterns used throughout the
    repository to ensure database consistency.

    :param mocker: The pytest-mock fixture.
    :type mocker: pytest_mock.plugin.MockerFixture
    :return: A tuple containing the (mock_pool, mock_conn).
    :rtype: Tuple[AsyncMock, AsyncMock]
    """
    mock_pool = mocker.AsyncMock(spec=asyncpg.Pool)
    mock_conn = mocker.AsyncMock(spec=asyncpg.Connection)
    mock_transaction = mocker.AsyncMock()

    # Wire the nested context managers together for realistic async interaction
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
    mock_conn.transaction.return_value.__aenter__.return_value = mock_transaction

    return mock_pool, mock_conn


# --- TEST CASES ---

@pytest.mark.asyncio
async def test_transition_to_running_success(mock_db_pool: Tuple[AsyncMock, AsyncMock]) -> None:
    """
    Verifies that a job correctly transitions to the 'RUNNING' state.

    Ensures that the repository accurately persists the dynamic reducer count
    and the physical data partition count into the PostgreSQL record, while
    successfully instantiating the updated Job domain model without relying
    on explicit class name fields.

    :param mock_db_pool: The mocked database pool and connection.
    :type mock_db_pool: Tuple[AsyncMock, AsyncMock]
    :return: None
    :rtype: None
    """
    mock_pool, mock_conn = mock_db_pool
    job_id = str(uuid.uuid4())

    # Alignment with new Multi-Tenant Job Schema (No explicit class names)
    mock_record = {
        "id": uuid.UUID(job_id),
        "user_id": uuid.uuid4(),
        "status": "RUNNING",
        "format": "JSON",
        "input_filename": "test_data.json",
        "mapper_filename": "WordCountMapper.class",
        "reducer_filename": "WordCountReducer.class",
        "num_reducers": 5,
        "total_chunks": 10,
        "completed_map_chunks": 0,
        "completed_reduce_chunks": 0
    }
    mock_conn.fetchrow.return_value = mock_record

    # Act: Verify the resolved partition count (5) is passed to the DB
    result = await JobRepository.transition_job_to_running(job_id, 10, 5, mock_pool)

    # Assert
    assert isinstance(result, Job)
    assert result.status == "RUNNING"
    assert result.total_chunks == 10
    assert result.num_reducers == 5
    assert result.mapper_filename == "WordCountMapper.class"
    mock_conn.fetchrow.assert_called_once()


@pytest.mark.asyncio
async def test_get_job_metadata_success(mock_db_pool: Tuple[AsyncMock, AsyncMock]) -> None:
    """
    Validates the retrieval of job metadata required for phase transitions.

    Ensures that get_job_metadata pulls all critical fields needed by the
    orchestration consumer, including the user_id for multi-tenant pathing
    and filenames for dynamic class derivation.

    :param mock_db_pool: The mocked database pool and connection.
    :type mock_db_pool: Tuple[AsyncMock, AsyncMock]
    :return: None
    :rtype: None
    """
    mock_pool, mock_conn = mock_db_pool
    job_id = str(uuid.uuid4())
    user_id = uuid.uuid4()

    mock_record = {
        "user_id": user_id,
        "total_chunks": 6,
        "reducer_filename": "reducer.class",
        "num_reducers": 2
    }
    mock_conn.fetchrow.return_value = mock_record

    result = await JobRepository.get_job_metadata(job_id, mock_pool)

    assert result is not None
    assert result["user_id"] == user_id
    assert result["num_reducers"] == 2
    assert result["reducer_filename"] == "reducer.class"
    mock_conn.fetchrow.assert_called_once()


@pytest.mark.asyncio
async def test_flush_phase_barriers_success(mock_db_pool: Tuple[AsyncMock, AsyncMock]) -> None:
    """
    Validates bulk flushes for Map and Reduce phase barriers.

    Ensures repository supports efficient synchronization of worker progress
    from the ephemeral cache to the persistent record.

    :param mock_db_pool: The mocked database pool and connection.
    :type mock_db_pool: Tuple[AsyncMock, AsyncMock]
    :return: None
    :rtype: None
    """
    mock_pool, mock_conn = mock_db_pool
    job_id = str(uuid.uuid4())

    # Verify both phases can be flushed independently
    await JobRepository.flush_map_phase(job_id, 6, mock_pool)
    await JobRepository.flush_reduce_phase(job_id, 2, mock_pool)

    assert mock_conn.execute.call_count == 2


@pytest.mark.asyncio
async def test_get_stalled_jobs_success(
    mock_db_pool: Tuple[AsyncMock, AsyncMock]
) -> None:
    """
    Tests the watchdog reclamation query execution.

    Verifies that the sweep executes successfully and directly runs the
    read-only query. Note: Testing for active-active Distributed Lock
    leadership is now strictly handled in test_watchdog.py via Redis.

    :param mock_db_pool: The mocked database pool and connection.
    :type mock_db_pool: Tuple[AsyncMock, AsyncMock]
    :return: None
    :rtype: None
    """
    mock_pool, mock_conn = mock_db_pool
    stalled_id = uuid.uuid4()

    # Simulate records returned by the expiration query
    mock_records = [{"id": stalled_id}]
    mock_conn.fetch.return_value = mock_records

    result = await JobRepository.get_stalled_jobs(3600, mock_pool)

    assert len(result) == 1
    assert result[0] == str(stalled_id)
    mock_conn.fetch.assert_called_once()
    # Explicitly ensure the legacy Postgres lock is no longer executed
    mock_conn.fetchval.assert_not_called()