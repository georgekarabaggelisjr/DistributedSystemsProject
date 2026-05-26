"""
Integration tests for the PostgreSQL Job Repository.

This suite utilizes Docker Testcontainers to provision ephemeral PostgreSQL
instances, ensuring the Data Access Layer (DAL) logic is validated against
a real database engine. It focuses on state transitions, metadata retrieval,
and the newly integrated Mapper/Reducer topology overrides.
"""

import uuid
import logging

import asyncpg
import pytest
import pytest_asyncio
from fastapi import HTTPException
from testcontainers.postgres import PostgresContainer

from app.repositories.job_repository import JobRepository

# Initialize logger for test lifecycle events
logger = logging.getLogger(__name__)


# --- FIXTURES ---

@pytest.fixture(scope="module")
def postgres_container():
    """
    Spins up a temporary Postgres Docker container for the test module.
    """
    with PostgresContainer("postgres:15-alpine") as postgres:
        yield postgres


@pytest_asyncio.fixture(scope="function")
async def db_pool(postgres_container):
    """
    Creates an asynchronous connection pool and initializes the test schema.

    This fixture executes the DDL to recreate the 'jobs' table before every test.
    It has been updated to include the 'num_mappers' column to support the
    Capacity Override feature.
    """
    db_url = postgres_container.get_connection_url().replace("postgresql+psycopg2", "postgresql")

    pool = await asyncpg.create_pool(db_url)

    async with pool.acquire() as conn:
        # Schema definition MUST match the refactored production init-db.sql
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS public.jobs (
                id UUID PRIMARY KEY,
                user_id UUID NOT NULL,
                status VARCHAR(50),
                format VARCHAR(50),
                input_filename VARCHAR(255),
                mapper_filename VARCHAR(255),
                reducer_filename VARCHAR(255),
                num_reducers INT NOT NULL DEFAULT 3,
                num_mappers INT DEFAULT NULL, -- FIXED: Added missing column
                total_chunks INT DEFAULT 0,
                completed_map_chunks INT DEFAULT 0,
                completed_reduce_chunks INT DEFAULT 0,
                updated_at TIMESTAMP DEFAULT NOW()
            );
            TRUNCATE TABLE public.jobs;
        """)
    yield pool
    await pool.close()


# --- TEST CASES ---

@pytest.mark.asyncio
async def test_transition_job_to_running_with_mapper_override(db_pool):
    """
    Verifies that explicit mapper overrides are correctly persisted to the DB.
    """
    job_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO public.jobs (id, user_id, status, format, input_filename, mapper_filename, reducer_filename)
            VALUES ($1, $2, 'SUBMITTED', 'JSON', 'data.json', 'Mapper.class', 'Reducer.class')
            """,
            uuid.UUID(job_id),
            uuid.UUID(user_id)
        )

    # Act: Transition with an explicit 15-mapper override
    updated_job = await JobRepository.transition_job_to_running(
        job_id=job_id,
        total_chunks=100,
        num_reducers=3,
        db_pool=db_pool,
        num_mappers=15
    )

    # Assert: Verify both scaling parameters saved correctly
    assert updated_job.status == "RUNNING"
    assert updated_job.num_mappers == 15
    assert updated_job.num_reducers == 3


@pytest.mark.asyncio
async def test_get_job_recovery_context_includes_mappers(db_pool):
    """
    Ensures that recovery context includes mapper overrides for the Watchdog.
    """
    job_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())

    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO public.jobs (id, user_id, status, num_mappers) VALUES ($1, $2, 'RUNNING', 25)",
            uuid.UUID(job_id), uuid.UUID(user_id)
        )

    context = await JobRepository.get_job_recovery_context(job_id, db_pool)

    assert context is not None
    assert context['num_mappers'] == 25


@pytest.mark.asyncio
async def test_transition_job_fails_if_not_submitted(db_pool):
    """
    Verifies Fail-Fast protocol for invalid state transitions.
    """
    job_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())

    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO public.jobs (id, user_id, status) VALUES ($1, $2, 'RUNNING')",
            uuid.UUID(job_id), uuid.UUID(user_id)
        )

    with pytest.raises(HTTPException) as exc_info:
        # This will now correctly attempt the update including num_mappers (None by default)
        await JobRepository.transition_job_to_running(job_id, 10, 3, db_pool)

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_flush_phases_update_counts(db_pool):
    """
    Validates bulk-flush mechanism for phase barriers.
    """
    job_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())

    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO public.jobs (id, user_id, status) VALUES ($1, $2, 'RUNNING')",
            uuid.UUID(job_id), uuid.UUID(user_id)
        )

    await JobRepository.flush_map_phase(job_id, 50, db_pool)
    await JobRepository.flush_reduce_phase(job_id, 10, db_pool)

    async with db_pool.acquire() as conn:
        record = await conn.fetchrow(
            "SELECT completed_map_chunks, completed_reduce_chunks FROM jobs WHERE id = $1",
            uuid.UUID(job_id)
        )

    assert record['completed_map_chunks'] == 50
    assert record['completed_reduce_chunks'] == 10


@pytest.mark.asyncio
async def test_get_stalled_jobs_read_only(db_pool):
    """
    Verifies watchdog sweep accuracy.
    """
    stalled_job = str(uuid.uuid4())
    user_id = str(uuid.uuid4())

    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO public.jobs (id, user_id, status, updated_at) VALUES ($1, $2, 'RUNNING', NOW() - INTERVAL '2 hours')",
            uuid.UUID(stalled_job), uuid.UUID(user_id)
        )

    stalled_ids = await JobRepository.get_stalled_jobs(3600, db_pool)

    assert len(stalled_ids) == 1
    assert stalled_ids[0] == stalled_job