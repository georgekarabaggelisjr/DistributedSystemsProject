"""
Global watchdog service for MapReduce job timeout monitoring.

This module provides a background monitoring service that ensures system
resilience by identifying and reclaiming resources from stalled MapReduce jobs.
It prevents resource leakage in the Kubernetes cluster by forcefully terminating
jobs that exceed the defined `GLOBAL_JOB_TIMEOUT`.

Performance & Active-Active Safety:
- **Concurrent Garbage Collection:** Utilizes `asyncio.gather` to concurrently
    tear down multiple stalled jobs, preventing K8s API and DB bottlenecks
    during massive failure events.
- **Leader Election:** Enforces a Redis-backed distributed lock to ensure
    only one Manager instance performs the Watchdog sweep per cycle, preventing
    race conditions in Active-Active multi-manager deployments.
- **Eventual Consistency:** Prioritizes physical resource deletion before
    mutating persistent database state to prevent orphaned pods.
- **Global Semaphore Leak Protection:** Enforces capacity release
    during zombie reclamation, ensuring the multi-tenant cluster mathematically
    recovers its available allocations.
"""

import logging
import asyncio

from app.core.database import get_db_pool
from app.core.config import settings
from app.k8s.client import get_k8s_provider
from app.repositories.job_repository import JobRepository
from app.core.redis import get_redis_client
from app.services.lifecycle import forcefully_reclaim_job

# Initialize the module-level logger for watchdog events
logger = logging.getLogger(__name__)


async def start_watchdog() -> None:
    """
    Background loop that sweeps for and terminates stalled MapReduce jobs.

    This service operates independently of the primary HTTP request/response
    lifecycle. It executes a periodic reconciliation loop using a Leader Election
    lock to guarantee Active-Passive execution across multiple Manager instances.

    Fault Tolerance:
        If the Manager pod crashes during cleanup, or K8s is temporarily
        unavailable, the database record remains 'RUNNING', guaranteeing
        a retry on the next Watchdog sweep.

    Raises:
        Exception: Broad exception handling is applied at the cycle level to
            ensure transient connectivity issues do not terminate the daemon.
    """
    logger.info(f"Watchdog service initialized (Timeout limit: {settings.GLOBAL_JOB_TIMEOUT}s).")

    # Initial delay to allow the application to fully boot and establish DB connections
    await asyncio.sleep(10)

    while True:
        try:
            redis_client = await get_redis_client()

            # --- ACTIVE-ACTIVE LEADER ELECTION ---
            # Attempt to acquire a distributed lock.
            # ex=60 ensures the lock auto-expires when the loop cycle finishes,
            # allowing rapid failover if the leading Manager instance crashes.
            is_leader = await redis_client.set(
                "cluster:watchdog_leader_lock",
                "LOCKED",
                nx=True,
                ex=60
            )

            if is_leader:
                db_pool = await get_db_pool()
                k8s_provider = await get_k8s_provider()

                stalled_job_ids = await JobRepository.get_stalled_jobs(
                    timeout_seconds=settings.GLOBAL_JOB_TIMEOUT,
                    db_pool=db_pool
                )

                if stalled_job_ids:
                    logger.info(
                        f"Watchdog Leader acquired lock. "
                        f"Executing concurrent sweep for {len(stalled_job_ids)} stalled jobs."
                    )

                    # Optimized implementation with a bounded semaphore to avoid DDOS
                    sem = asyncio.Semaphore(50)

                    async def _bounded_reclaim(job_id: str):
                        async with sem:
                            # Delegated to the centralized lifecycle service
                            await forcefully_reclaim_job(job_id, db_pool, k8s_provider, redis_client)

                    cleanup_tasks = [_bounded_reclaim(job_id) for job_id in stalled_job_ids]
                    await asyncio.gather(*cleanup_tasks)

            else:
                logger.debug("Watchdog leader lock held by another instance. Bypassing sweep.")

        except Exception as e:
            logger.error(f"Watchdog service encountered a cycle error: {str(e)}")

        # Hibernate for 60 seconds before the next sweep
        await asyncio.sleep(60)